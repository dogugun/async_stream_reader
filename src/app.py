from flask import Flask
from flask import Flask, jsonify

from api_module.calculations import get_user_aggregations, get_recent_shows, get_successful_streams, get_start_event_rate
from werkzeug.exceptions import HTTPException

from variables.variables import ERROR_MESSAGE_404

app = Flask(__name__)

@app.route('/user_report')
def get_user_report():
    person_list = get_user_aggregations()
    return jsonify(person_list)

@app.route('/recent_shows')
def recent_shows():
    return get_recent_shows(year=2020)

@app.route('/successful_streams')
def successful_streams():
    return get_successful_streams()


@app.route('/start_events')
def start_events():
    try:
        res = get_start_event_rate()
        return res
    except (HTTPException):
        return ERROR_MESSAGE_404



def start_api_from_ep():
    app.run()

# main driver function
if __name__ == "__main__":
    app.run()