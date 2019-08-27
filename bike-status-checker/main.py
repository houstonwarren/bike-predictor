import requests
import json
import pymysql
from pymysql.err import OperationalError
from os import getenv
from datetime import datetime
from google.cloud import pubsub_v1


MODE = getenv('mode')
DB_CONN = getenv('db_conn')
DB_HOST = getenv('db_host')
DB_USER = getenv('db_user')
DB_PASSWORD = getenv('db_pass')
DB_NAME = getenv('db_name')
WEATHER_KEY = getenv('weather_key')


def update_status(data, context):

    # get station info
    stations = json.loads(requests.get('https://gbfs.citibikenyc.com/gbfs/es/station_status.json').content)
    for station in stations['data']['stations']:
        if station['station_id'] == '445':
            home = station
    print(f"bikes available: {home['num_bikes_available']}")

    # get weather info
    weather = json.loads(requests.get(f'http://api.openweathermap.org/data/2.5/forecast?id=5128638&appid={WEATHER_KEY}')
                         .content)['list'][3] # this should be 12 hours ahead
    forecast_time = datetime.fromtimestamp(weather['dt'])
    # insert kelvin conversion here
    weather_dict = {'temp': weather['main']['temp'], 'pressure':weather['main']['pressure'],
                    'humidity':weather['main']['humidity'],'wind_speed': weather['wind']['speed'],
                    'wind_deg':weather['wind']['deg'],'clouds_all':weather['clouds']['all'],
                    'weather_main': weather['weather'][0]['main'],'hour': forecast_time.hour,
                    'year':forecast_time.year,'month':forecast_time.month, 'day': forecast_time.day}

    # write to database
    with get_cursor() as cursor:
        status_insert_statement = f'''
            INSERT INTO statuses (`time`, avail_bikes, avail_docks,avail_ebikes,disabled_bikes,disabled_docks) VALUES
            ( NOW(), {home['num_bikes_available']},
                {home['num_docks_available']},
                {home['num_ebikes_available']},
                {home['num_bikes_disabled']},
                {home['num_docks_disabled']}
            );
        '''

        weather_insert_statement = f'''
            INSERT INTO weather_forecasts (
                `time`,temp,pressure,humidity,wind_speed,wind_deg,clouds_all,weather_main,hour,year,month,day
            ) 
            VALUES (
                NOW(),
                {weather_dict['temp']},
                {weather_dict['pressure']},
                {weather_dict['humidity']},
                {weather_dict['wind_speed']},
                {weather_dict['wind_deg']},
                {weather_dict['clouds_all']},
                '{weather_dict['weather_main']}',
                {weather_dict['hour']},
                {weather_dict['year']},
                {weather_dict['month']},
                {weather_dict['day']}
            );
        '''

        cursor.execute(status_insert_statement)
        cursor.execute(weather_insert_statement)

    # invoke model run using gcloud pubsub
    push_to_pubsub()

    return 'complete'


def push_to_pubsub():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('citi-244805', 'runmodel-pubsub')
    data = '{}'.encode('utf-8')
    publisher.publish(topic_path, data=data)


def get_cursor():
    """
    cursor function for database access. Taken from cloud function documentation

    :return: pymysql.connect.cursor
    """

    if MODE == 'PROD':
        mysql_config = {
            'host': DB_HOST,
            'user': DB_USER,
            'password': DB_PASSWORD,
            'db': DB_NAME,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
            'autocommit': True,
            'unix_socket': f'/cloudsql/{DB_CONN}'
        }

    else:
        mysql_config = {
            'host': DB_HOST,
            'user': DB_USER,
            'password': DB_PASSWORD,
            'db': DB_NAME,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
            'autocommit': True
        }

    try:
        conn = pymysql.connect(**mysql_config)
        return conn.cursor()
    except OperationalError:
        conn = pymysql.connect(**mysql_config)
        conn.ping(reconnect=True)
        return conn.cursor()
