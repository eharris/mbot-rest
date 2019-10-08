#!/usr/bin/python3

"""REST API for leaderboard with weather data
"""

import os
import sys
import json
import datetime

from flask import Flask, jsonify

# Hardcoded defines, these should eventually be configured elsewhere
leaderboard_file = "data/leaderboard.json"
weather_file = "data/station_observations.json"

# Supported time string formats
ts_formats = [
    "%Y-%m-%dT%H:%M:%S+00:00",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
]

app = Flask(__name__)

class InvalidUsage(Exception):
    """Simple Exception class for Flask

    This allows for returning more useful error information for 
    failed API requests.
    """

    status_code = 400

    def __init__(self, message, status_code = None, payload = None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    "Default error handler for failed API requests"
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

def ts_parse(ts: str) -> datetime:
    "Decode time string in any allowed format to datetime"
    for fmt in ts_formats:
        try:
            dt = datetime.datetime.strptime(ts, fmt)
            break
        except:
            continue
    else:
        #raise ValueError("Unknown time string format", ts)
        # Simpler to return None than have to process errors
        return None
    return dt

def read_json_dict(fname) -> dict:
    """Read a dict from a JSON file with given filename

    Falls back to returning an empty dict if there are any errors.
    """
    #try:
    with open(fname, 'r') as f:
        data = json.load(f)
    #except:
    #    return {}
    return data

def weather_find(dt: datetime, events: dict) -> dict:
    """Return the weather details closest to the given time

    The passed events is a multi-level dict of weather events.
    The returned dict contains the entire 'properties' dict of the
    closest weather observation.
    """
    nearest = None
    weather = None

    for event in events['features']:
        dte = ts_parse(event['properties']['timestamp'])
        if nearest is None or abs(dt - dte) < nearest:
            nearest = abs(dt - dte)
            weather = event['properties']
    return weather

@app.route("/leaders/timerange/<ts1>/<ts2>", methods = ["GET"])
def leaders_timerange(ts1, ts2) -> str:
    """API Endpoint: Get list of events that occur between two timestamps

    URL: /leaders/timerange/<ts1>/<ts2>

    Events are returned as a JSON representation of a list of events
    (dicts).
    The two timestamps are inclusive and events are sorted according to
    the top level dict keys.
    """
    dt1 = ts_parse(ts1)
    dt2 = ts_parse(ts2)
    if dt1 is None or dt2 is None or dt2 < dt1:
        raise InvalidUsage("Error parsing timestamp parameters")

    events = read_json_dict(leaderboard_file)

    # Filter events by time (sorted)
    retlist = []
    for event in sorted(events):
        dt = ts_parse(events[event]['start_date'])
        if dt >= dt1 and dt <= dt2:
            retlist.append(events[event])
    return jsonify(retlist)

@app.route("/leaders/temprange/<temp1>/<temp2>", methods = ["GET"])
def leaders_temprange(temp1, temp2) -> str:
    """API Endpoint: Get list of events that occur between two temperatures

    URL: /leaders/temprange/<temp1>/<temp2>

    Events are returned as a JSON representation of a list of events
    (dicts).
    The two temperatures are given as floats, are inclusive, and events
    are sorted according to the top level dict keys.
    """
    try:
        temp1 = float(temp1)
        temp2 = float(temp2)
    except ValueError:
        raise InvalidUsage("Invalid temperature parameter")
        
    if temp2 < temp1:
        raise InvalidUsage("First temp must be less than or equal to second")

    events = read_json_dict(leaderboard_file)
    wevents = read_json_dict(weather_file)

    # Filter events by temp
    retlist = []
    for event in sorted(events):
        dt = ts_parse(events[event]['start_date'])
        weather = weather_find(dt, wevents)
        temp = weather['temperature']['value']
        if temp >= temp1 and temp <= temp2:
            #print("ETS:", dt, "WTS:", weather['timestamp'], "Temp:", temp)
            retlist.append(events[event])
    return jsonify(retlist)

@app.route("/weather/<ts>", methods = ["GET"])
def weather_at_time(ts) -> str:
    """API Endpoint: Get weather details nearest given time

    URL: /weather/<ts>

    Events are returned as a JSON representation of the full weather
    details (as provided by weather source data).
    """
    dt = ts_parse(ts)
    if dt is None:
        raise InvalidUsage("Error parsing timestamp parameter")

    events = read_json_dict(weather_file)
    weather = weather_find(dt, events)
    return jsonify(weather)

if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 5000, threaded = True)


