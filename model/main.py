from os import getenv
import pymysql
from pymysql.err import OperationalError
from datetime import datetime, timedelta
import pickle
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
import numpy as np

MODE = getenv('mode')
DB_HOST = getenv('db_host')
DB_USER = getenv('db_user')
DB_PASSWORD = getenv('db_pass')
DB_NAME = getenv('db_name')
DB_CONN = getenv('db_conn')
WEATHER_KEY = getenv('weather_key')


# TODO make sure no missing values
# TODO write the prediction upload function
# TODO FIX ERROR ON DATABASE/APP TIMEZONE


def run_all(data, context):
    weather_raw = get_weather()
    status_raw = get_status()

    if MODE == 'PROD':
        # correct for different timezone on database
        dt = get_forecast_time() + timedelta(hours=-4)
    else:
        dt = get_forecast_time()
    sig_lags = get_sig_lags('sig_lags.p')
    status_vec = create_status_array(status_raw, sig_lags, dt)
    weather_vec = create_weather_array(weather_raw)

    X = combine_status_weather(status_vec, weather_vec)
    pred = run_model('station_rf.p', X)

    write_model_outputs(pred, dt)

    print(pred)


def run_model(model_file, x_obs):
    with open(model_file, 'rb') as file:
        model = pickle.load(file)

    pred = model.predict_proba(x_obs)
    return pred[0][1]


def write_model_outputs(pred, dt):
    with get_cursor() as cursor:
        sql_string = f'''
            INSERT
            INTO predictions (forecast_time, prediction) 
            VALUES (
                '{dt.strftime('%Y-%m-%d %H-%M-%S')}',
                {pred}
            );
        '''

        cursor.execute(sql_string)


def get_sig_lags(file_name):
    with open(file_name, 'rb') as file:
        sig_lags = pickle.load(file)
    return sig_lags


def get_forecast_time():
    dt = datetime.now() + timedelta(hours=12)
    dt = datetime(dt.year, dt.month, dt.day, dt.hour, 15 * (dt.minute // 15))
    return dt


def get_weather():
    with get_cursor() as cursor:
        # get past month of status
        weather_string = '''
            select * 
            from weather_forecasts 
            order by time desc limit 1
        '''
        cursor.execute(weather_string)
        weather_rows = cursor.fetchall()

    return weather_rows


def get_status():
    with get_cursor() as cursor:
        # get past month of status
        status_string = '''
            select * 
            from statuses 
            order by time desc limit 2880
        '''
        cursor.execute(status_string)
        status_rows = cursor.fetchall()

    return status_rows


def create_weather_array(weather_raw):
    weather_types = ['Clear', 'Clouds', 'Drizzle', 'Dust', 'Fog', 'Haze', 'Mist',
                     'Rain', 'Sand', 'Smoke', 'Snow', 'Squall', 'Thunderstorm']

    cur_weather = weather_raw[0]
    weather_vec = [cur_weather['temp'], cur_weather['pressure'], cur_weather['humidity'], cur_weather['wind_speed'],
                   cur_weather['wind_deg'], cur_weather['clouds_all']]
    weather_types_one_hot = len(weather_types) * [0]
    weather_types_one_hot[weather_types.index(cur_weather['weather_main'])] = 1
    weather_vec_fin = weather_vec + weather_types_one_hot
    return weather_vec_fin


def create_status_array(status_raw, sig_lags, dt):
    if dt.weekday() < 5:
        weekday = 1
    else:
        weekday = 0

    sig_lag_values = [status_raw[sig_lag]['avail_bikes'] for sig_lag in sig_lags]
    time_values = [dt.hour, dt.year, dt.month, dt.day, dt.minute, weekday]
    status_vec = time_values + sig_lag_values
    return status_vec


def combine_status_weather(status, weather):
    X = np.array(status + weather).reshape(1, -1)
    return X


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
