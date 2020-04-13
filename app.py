import numpy as np
import datetime as dt

import sqlalchemy 
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Database Setup
#engine = create_engine("sqlite:////Users/shui/Desktop/BSC/UT-TOR-DATA-PT-01-2020-U-C-Master/10-Advanced-Data-Storage-and-Retrieval/Homework/sqlalchemy-challenge/Resources/hawaii.sqlite")
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect = True)
Base.classes.keys()
# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station 


app = Flask(__name__)

# flask route

@app.route("/")
def Home():
    session = Session(engine)

    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = dt.date(int(last_date[0].split('-',1)[0]),int(last_date[0].split('-',2)[1]), int(last_date[0].split('-',2)[2]))
    first_date = session.query(Measurement.date).order_by(Measurement.date).first()
    earliest_date = dt.date(int(first_date[0].split('-',1)[0]),int(first_date[0].split('-',2)[1]), int(first_date[0].split('-',2)[2]))
    
    session.close()

    return (
        f"Available Hawaii Weather API!<br/>"
        f"Available Routes (dates between {earliest_date} to {latest_date}):<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/yyyy-mm-dd <br/>"
        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # query all precipitation data from previous year 
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = dt.date(int(last_date[0].split('-',1)[0]),int(last_date[0].split('-',2)[1]), int(last_date[0].split('-',2)[2]))
    date_last_year = latest_date - dt.timedelta(days = 365)
    results = session.query(Measurement.date, Measurement.prcp).\
              filter(Measurement.date >= date_last_year).all()
    session.close()
    # create a dictionary
    all_prcp = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        all_prcp.append(prcp_dict)
    return jsonify(all_prcp)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    results = session.query(Measurement.station).\
              group_by(Measurement.station).all()
    session.close()
    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))
    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = dt.date(int(last_date[0].split('-',1)[0]),int(last_date[0].split('-',2)[1]), int(last_date[0].split('-',2)[2]))
    date_last_year = latest_date - dt.timedelta(days = 365)
    active_stations = session.query(Measurement.station,func.count(Measurement.tobs)).\
                      group_by(Measurement.station).\
                      order_by(func.count(Measurement.tobs).desc()).first()
    most_active_station = active_stations[0]
    results = session.query(Measurement.date, Measurement.tobs).\
              filter(Measurement.date >= date_last_year).\
              filter(Measurement.station == most_active_station).all()
    session.close()

    all_tobs = []
    for date, tobs in results:
        tobs_dict = {}
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        all_tobs.append(tobs_dict)
    return jsonify(all_tobs)

@app.route("/api/v1.0/<start_date>")
def temp_after_date(start_date):
    session = Session(engine)
    start_period = dt.date(int(start_date.split('-',1)[0]),int(start_date.split('-',2)[1]), int(start_date.split('-',2)[2]))
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    for date_exist in list(np.ravel(session.query(Measurement.date).all())):
        date_available = dt.date(int(date_exist.split('-',1)[0]),int(date_exist.split('-',2)[1]), int(date_exist.split('-',2)[2]))
        if date_available == start_period:
            results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                      filter(Measurement.date >= start_period).all()
        
    session.close()
    temp_info_after_date= {"start_date" : start_date, "end_date" : last_date[0], "tmin": list(np.ravel(results))[0],\
                          "tavg" : list(np.ravel(results))[1], "tmax": list(np.ravel(results))[2]}
    return jsonify(temp_info_after_date)

@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_between_date(start_date, end_date):
    session = Session(engine)
    start_period = dt.date(int(start_date.split('-',1)[0]),int(start_date.split('-',2)[1]), int(start_date.split('-',2)[2]))
    end_period = dt.date(int(end_date.split('-',1)[0]), int(end_date.split('-',2)[1]), int(end_date.split('-',2)[2]))

    #for date_exist in list(np.ravel(session.query(Measurement.date).all())):
        #date_available = dt.date(int(date_exist.split('-',1)[0]),int(date_exist.split('-',2)[1]), int(date_exist.split('-',2)[2]))
        #if date_available == start_period:
            #if date_available == end_period:
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
              filter(Measurement.date >= start_period).\
              filter(Measurement.date <= end_period).all()       
    session.close()
    temp_info_between_date= {"start_date" : start_date, "end_date" : end_date, \
                            "tmin": list(np.ravel(results))[0], "tavg" : list(np.ravel(results))[1], "tmax": list(np.ravel(results))[2]}   
    return jsonify(temp_info_between_date)

if __name__ == "__main__":
    app.run(debug=True)
