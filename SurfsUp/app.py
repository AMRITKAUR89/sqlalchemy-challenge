# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
import pandas as pd

#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(autoload_with=engine)

# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station`
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session
session = Session(engine)

#################################################
# Flask Setup
#################################################
# 1. import Flask
from flask import Flask, jsonify

# 2. Create an app, being sure to pass __name__
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# Question1. 
# Start at the homepage.
# List all the available routes.

@app.route("/")
def home():        
    print("Server received request for 'Home' page...")
    """List all available api routes."""
    return (
        "Welcome to my 'Home' page!"
        f"<br>Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt; (format: YYYYMMDD)<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt; (format: YYYYMMDD/YYYYMMDD)<br/>"
    )

# Question2.
# Convert the query results to a dictionary by using date as the key and prcp as the value.
# Return the JSON representation of your dictionary.

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Query date and precipitation data
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    most_recent_date = dt.datetime.strptime(most_recent_date, "%Y-%m-%d")

# Calculate the date one year from the last date in data set.
    one_year_ago = most_recent_date - dt.timedelta(days=365)

# Perform a query to retrieve the data and precipitation scores
    precip_data = (
    session.query(Measurement.date, Measurement.prcp)
    .filter(Measurement.date >= one_year_ago, Measurement.date <= most_recent_date)
    .order_by(Measurement.date)  # Order by date in ascending order
    .all()
)
    precip_data_df = pd.DataFrame(precip_data, columns=['Date', 'Precipitation'])
    precip_data_df = precip_data_df.sort_values(by='Date')

    # precipitation_data = precip_data_df.set_index('Date')['Precipitation'].to_dict()   
    precipitation_data = precip_data_df.to_dict(orient="records")
    print("Server received request for 'precipitation' page...")
    
    # Return JSON response
    return jsonify(precipitation_data)


# Question3.
# Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def station_names():
    results = session.query(Station.station).all()
# Convert list of tuples into normal list
    all_names = list(np.ravel(results))
    
    return jsonify(all_names)

# Question 4. 
# Query the dates and temperature observations of the most-active station for the previous year of data.
# Return a JSON list of temperature observations for the previous year.
@app.route("/api/v1.0/tobs")
def dt_obs():
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    most_recent_date = dt.datetime.strptime(most_recent_date, "%Y-%m-%d")
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    most_active_station_id = most_active_stations[0][0]
    
    one_year_ago = most_recent_date - dt.timedelta(days=365)
    temperature_data = (
    session.query(Measurement.date, Measurement.tobs)
    .filter(Measurement.station == most_active_station_id)
    .filter(Measurement.date >= one_year_ago)
    .all()
)

    temp_df = pd.DataFrame(temperature_data, columns=['Date', 'Temperature'])
    temp_data = temp_df.to_dict(orient="records")

    return jsonify(temp_data)

#Question 5.
# Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.
# For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
# For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.
@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def min_temp(start=None, end=None):
    # Parse start and end dates
    start = dt.datetime.strptime(start, "%Y%m%d")
    end = dt.datetime.strptime(end, "%Y%m%d") if end else None

    # Calculate TMIN, TAVG, and TMAX for the date range
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]

    # Perform the query based on whether an end date is provided
    if end:
        # Query data between start and end dates
        results = session.query(*sel).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    else:
        # Query data from the start date onwards if end date is not provided
        results = session.query(*sel).filter(Measurement.date >= start).all()

    # Close the session
    session.close()

    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))

    # Return JSON response
    return jsonify({
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d") if end else "N/A",
        "min_temp": temps[0],
        "avg_temp": temps[1],
        "max_temp": temps[2]
    })

    


if __name__ == "__main__":
    app.run(debug=True)







    
