from flask import Flask, render_template
from db_funcs import get_predictions


app = Flask(__name__)


@app.route('/')
def index():
    predictions = get_predictions()
    return render_template("index.html", predictions=predictions)


@app.route('/model')
def explained():
    return render_template("notebook.html")


# We only need this for local development.
if __name__ == '__main__':
    app.run()
