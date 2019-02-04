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
    #all_data = pd.read_sql("SELECT * FROM station", conn)
    #stations2 = all_data.to_dict()
    #dfstationsToList = stations2['station'].tolist()
    #station_count3 = session.query(Station.station.all())
    station4 = engine.execute('SELECT station FROM station').fetchall()
    station5 = [i[0] for i in station4]
    #dfstationsToList = station4.to_dict()
    return jsonify(station5)

@app.route("/api/v1.0/tobs")
def tobs():
    #tobs3 = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date.between('2016-08-23', '2017-08-23')).all()
    tobs4 = engine.execute("SELECT date,tobs FROM measurement WHERE date between '2016-08-23' and '2017-08-23' ORDER BY date ASC").fetchall()
    #tobs_data_df = pd.read_sql("SELECT date,tobs measurement WHERE date between '2016-08-23' and '2017-08-23' ORDER BY date ASC", conn)
    #tobs4.columns = ['date', 'tobs']
    #tobs_df = pd.DataFrame(tobs3, columns=["date", "temperature"])
    #tobs_data_df.columns = ['date', 'tobs']
    #tobs_dict = tobs4.set_index('date')['tobs'].to_dict()
    tobs_dict1 = dict(tobs4)
    return jsonify(tobs_dict1)

@app.route("/api/v1.0/<start>")
def range_a(start):
    #return jsonify(tripduration)
    #import datetime
    #end = datetime.datetime(2012, 3, 5)
    #start = datetime.datetime(2012, 2, 28)
    #range_fulldates = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
    #                                func.max(Measurement.tobs)).filter(Measurement.date >= start).group_by(Measurement.date).all()
    #fulldates_dict = list(range_fulldates)
    #return jsonify(fulldates_dict)
    import datetime
    end = datetime.datetime(2012, 3, 5)
    start = datetime.datetime(2012, 2, 28)
    range_fulldates = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
                                    func.max(Measurement.tobs)).filter(Measurement.date >= start).group_by(
        Measurement.date).all()
    fulldates_dict = list(range_fulldates)
    start1 = [i[0] for i in fulldates_dict]
    start2 = [i[1] for i in fulldates_dict]
    start3 = [i[2] for i in fulldates_dict]
    start4 = [i[3] for i in fulldates_dict]
    start5df = pd.DataFrame(
        {'date': start1,
         'tavg': start2,
         'tmax': start4,
         'tmin': start3
         })
    start5df.reset_index()
    start5df.set_index('date', inplace=True)
    start6 = start5df.to_dict('index')
    return jsonify(start6)


@app.route("/api/v1.0/<start>/<end>")
def range_b(start,end):
    #import datetime
    #end = datetime.datetime(2012, 3, 5)
    #start = datetime.datetime(2012, 2, 28)
    #range_vacationdates = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
    #                                func.max(Measurement.tobs)).filter(Measurement.date.between(start, end)).group_by(Measurement.date).all()
    #range_vacationdates
    #return jsonify(range_fulldates)
    import datetime
    end = datetime.datetime(2012, 3, 5)
    start = datetime.datetime(2012, 2, 28)
    start_end_full = session.query(Measurement.date, func.avg(Measurement.tobs), func.min(Measurement.tobs),
                                    func.max(Measurement.tobs)).filter(Measurement.date.between(start, end)).group_by(
        Measurement.date).all()
    startend_list = list(start_end_full)
    startend1 = [i[0] for i in startend_list]
    startend2 = [i[1] for i in startend_list]
    startend3 = [i[2] for i in startend_list]
    startend4 = [i[3] for i in startend_list]
    startend15df = pd.DataFrame(
        {'date': startend1,
         'tavg': startend2,
         'tmax': startend3,
         'tmin': startend4
         })
    startend15df.reset_index()
    startend15df.set_index('date', inplace=True)
    startend16df = startend15df.to_dict('index')
    return jsonify(startend16df)

if __name__ == "__main__":
    app.run(debug=True)