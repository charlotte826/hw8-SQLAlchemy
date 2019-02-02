import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import create_engine, inspect

from sqlalchemy import func
import numpy as np
import pandas as pd

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
conn = engine.connect()

prcp_1 = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date.between('2016-08-23', '2017-08-23')).all()
prcp_df = pd.DataFrame(prcp_1,columns=["date","precipitation"])
prcp_df.dropna(how='any', inplace=True)
prcp_df.set_index('date', inplace=True, drop=True)
prcp_df.sort_values(by='date')

Base = automap_base()
Base.prepare(engine, reflect=True)
Station = Base.classes.station
session = Session(engine)
#results = session.query(Station.station).count()
results = session.query(func.count(Station.station)).all()
#results
station_count2 = session.query(Measurement.station,func.count(Measurement.station)).\
        group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
#station_count2
temperatures_q = session.query(func.min(Measurement.tobs),
                               func.max(Measurement.tobs),
                func.avg(Measurement.tobs),
                ).filter_by(station = "USC00519281").all()
#temperatures_q

session = Session(engine)
highstation_temp_date = session.query(Measurement.date, Measurement.tobs).filter_by(station = "USC00519281").\
    filter(Measurement.date.between('2016-08-18', '2017-08-18')).all()
#highstation_temp_date

highstation_temp = [int(result[1]) for result in highstation_temp_date]


def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.

    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d

    Returns:
        TMIN, TAVE, and TMAX
    """

    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)). \
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()


# function usage example
tripduration = calc_temps('2012-01-28', '2012-02-05')
tmin, tmax, tavg = calc_temps('2012-01-28', '2012-02-05')[0]
#print(tmin, tmax, tavg)

start_date = '2012-01-01'
end_date = '2012-01-07'
rainfall_per_station = session.query(Station.station,Station.name,
                                     Station.latitude, Station.longitude,
                                     Station.elevation,func.sum(Measurement.prcp)).\
    filter(Measurement.station == Station.station).\
    filter(Measurement.date >= start_date).\
    filter(Measurement.date <= end_date)\
    .group_by(Station.name).\
    order_by(func.sum(Measurement.prcp).desc()).all()
#rainfall_per_station


from flask import Flask, jsonify


app = Flask(__name__)

@app.route("/")
def welcome():
    # List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )


@app.route("/api/v1.0/precipitation")
#Convert the query results to a Dictionary using date as the key and prcp as the value.
# Return the JSON representation of your dictionary.
def precipitation():
    prcp_df2 = prcp_df.reset_index()
    prcp_df2.columns = ['date', 'prcp']
    abc = prcp_df2.set_index('date')['prcp'].to_dict()
    return jsonify(abc)

@app.route("/api/v1.0/stations")
def stations():
    dfstationsToList = all_data['station'].tolist()
    return jsonify(dfstationsToList)

@app.route("/api/v1.0/tobs")
def tobs():
    return jsonify(prcp_df)

@app.route("/api/v1.0/<start>")
def range_a(start):
    return jsonify(tripduration)
    import datetime
    end = datetime.datetime(2012, 3, 5)
    start = datetime.datetime(2012, 2, 28)
    range_fulldates = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
                                    func.max(Measurement.tobs)).filter(Measurement.date >= start).group_by(Measurement.date).all()
    range_fulldates
    return jsonify(range_fulldates)

@app.route("/api/v1.0/<start>/<end>")
def range_b(start,end):
    import datetime
    end = datetime.datetime(2012, 3, 5)
    start = datetime.datetime(2012, 2, 28)
    range_vacationdates = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
                                    func.max(Measurement.tobs)).filter(Measurement.date.between(start, end)).group_by(Measurement.date).all()
    range_vacationdates
    return jsonify(range_fulldates)

if __name__ == "__main__":
    app.run(debug=True)