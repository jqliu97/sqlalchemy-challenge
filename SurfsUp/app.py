# Import the dependencies.
from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
import datetime as dt
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

# Calculate the date one year from the last date in the dataset
most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')
one_year_ago = most_recent_date - dt.timedelta(days=365)

# Find the most active station
active_stations = session.query(Measurement.station, func.count(Measurement.station).label('station_count')).\
    group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc()).all()

most_active_station = active_stations[0].station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    return (
        "Welcome to the Climate App API!<br/><br/>"
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/tobs<br/>"
        "/api/v1.0/start_date<br/>"
        "/api/v1.0/start_date/end_date"
    )

# Route for /api/v1.0/precipitation
@app.route('/api/v1.0/precipitation')
def precipitation():
    precipitation_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).\
        order_by(Measurement.date).all()

    precipitation_df = pd.DataFrame(precipitation_data, columns=['Date', 'Precipitation'])
    precipitation_df = precipitation_df.sort_values(by='Date')
    precipitation_df['Date'] = pd.to_datetime(precipitation_df['Date'])
    precipitation_dict = precipitation_df.to_dict(orient='records')

    return jsonify(precipitation_dict)

# Route for /api/v1.0/stations
@app.route('/api/v1.0/stations')
def stations():
    stations_data = [{'Station': station.station, 'Count': station.station_count} for station in active_stations]
    return jsonify(stations_data)

# Route for /api/v1.0/tobs
@app.route('/api/v1.0/tobs')
def tobs():
    most_active_last_date = session.query(Measurement.date).\
        filter(Measurement.station == most_active_station).\
        order_by(Measurement.date.desc()).first()[0]

    most_active_last_date = dt.datetime.strptime(most_active_last_date, '%Y-%m-%d')
    most_active_one_year_ago = most_active_last_date - dt.timedelta(days=365)

    most_active_temperatures = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= most_active_one_year_ago).all()

    most_active_temperatures_df = pd.DataFrame(most_active_temperatures, columns=['Date', 'Temperature'])
    most_active_temperatures_df['Date'] = pd.to_datetime(most_active_temperatures_df['Date'])
    tobs_dict = most_active_temperatures_df.to_dict(orient='records')

    return jsonify(tobs_dict)

# Route for /api/v1.0/<start_date>
@app.route('/api/v1.0/<start_date>')
def start_date(start_date):
    temperature_stats = session.query(func.min(Measurement.tobs).label('min_temp'),
                                      func.avg(Measurement.tobs).label('avg_temp'),
                                      func.max(Measurement.tobs).label('max_temp')).\
        filter(Measurement.date >= start_date).all()

    temperature_dict = {'Min Temperature': temperature_stats[0].min_temp,
                        'Average Temperature': temperature_stats[0].avg_temp,
                        'Max Temperature': temperature_stats[0].max_temp}

    return jsonify(temperature_dict)

# Route for /api/v1.0/<start_date>/<end_date>
@app.route('/api/v1.0/<start_date>/<end_date>')
def start_end_date(start_date, end_date):
    temperature_stats = session.query(func.min(Measurement.tobs).label('min_temp'),
                                      func.avg(Measurement.tobs).label('avg_temp'),
                                      func.max(Measurement.tobs).label('max_temp')).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

    temperature_dict = {'Min Temperature': temperature_stats[0].min_temp,
                        'Average Temperature': temperature_stats[0].avg_temp,
                        'Max Temperature': temperature_stats[0].max_temp}

    return jsonify(temperature_dict)

# Run the app if this script is the main program
if __name__ == '__main__':
    app.run(debug=True)

