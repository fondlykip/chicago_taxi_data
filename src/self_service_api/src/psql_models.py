from sqlalchemy import (Boolean,
                        Column,
                        ForeignKey,
                        Integer,
                        String,
                        DateTime,
                        Float,
                        Time)
from geoalchemy2 import Geometry
from .psql_database import psqlBase

class TaxiTrips(psqlBase):
    __tablename__ = "chicago_taxi_trips_fact"
    
    trip_id = Column(String, primary_key=True, )
    taxi_id = Column(String)
    trip_start_timestamp = Column(DateTime)
    trip_start_date_id =Column(Integer)
    trip_start_time =Column(Time)
    trip_end_timestamp = Column(DateTime)
    trip_end_date_id = Column(Integer) 
    trip_end_time = Column(Time)
    trip_seconds = Column(Integer)
    trip_miles =Column(Float)
    pickup_census_tract = Column(String)
    dropoff_census_tract = Column(String)
    pickup_community_area = Column(Integer, ForeignKey('community_area_dim.community_area_id'))
    dropoff_community_area = Column(Integer, ForeignKey('community_area_dim.community_area_id'))
    fare = Column(Float)
    tips = Column(Float)
    tolls = Column(Float)
    extras = Column(Float)
    trip_total = Column(Float)
    payment_type = Column(String)
    company = Column(String)
    pickup_centroid_latitude = Column(Float)
    pickup_centroid_longitude = Column(Float)
    pickup_centroid_location = Column(Geometry('POINT'))
    dropoff_centroid_latitude = Column(Float)
    dropoff_centroid_longitude = Column(Float)
    dropoff_centroid_location =Column(Geometry('POINT'))

class CommunityAreas(psqlBase):
    __tablename__='community_area_dim'
    community_area_id = Column(Integer, primary_key=True)
    community_area_name = Column(String)
    community_area_size = Column(Float)