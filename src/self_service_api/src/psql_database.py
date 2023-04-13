from sqlalchemy import create_engine
from sqlalchemy.sql.expression import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, Depends
import os
from typing import List, Iterator, Any
#from fastapi_pagination import LimitOffsetPage, Page, add_pagination
#from fastapi_pagination.ext.sqlalchemy import paginate
from sqlalchemy import (Column,
                        ForeignKey,
                        Integer,
                        String,
                        DateTime,
                        Float,
                        Time, Numeric)
from geoalchemy2 import Geometry
from datetime import datetime
from geojson_pydantic.geometries import Point
from pydantic import BaseModel

PSQL_TRIP_DB_CONN_STR="postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips"

engine = create_engine(os.environ["PSQL_CONN_STR"])
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

psqlBase = declarative_base()

psqlApp = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
    
class TaxiModel(psqlBase):
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

class CommunityAreasModel(psqlBase):
    __tablename__='community_area_dim'
    community_area_id = Column(Integer, primary_key=True)
    community_area_name = Column(String)
    community_area_size = Column(Float)

class TripSchema(BaseModel):
    trip_id: str
    taxi_id: str
    trip_start_timestamp: datetime
    trip_end_timestamp: datetime
    trip_seconds: int
    trip_miles: float
    pickup_census_tract: str
    dropoff_census_tract: str
    pickup_community_area: int
    dropoff_community_area: int
    fare: float
    tips: float
    tolls: float
    extras: float
    trip_total: float
    payment_type: str
    company: str
    pickup_centroid_latitude: float
    pickup_centroid_longitude: float
    pickup_centroid_location: Point
    dropoff_centroid_latitude: float
    dropoff_centroid_longitude: float
    dropoff_centroid_location: Point

    class Congif:
        orm_mode=True
    
class CommunityAreaSchema(BaseModel):
    community_area_id: int
    community_area_name: str
    community_area_size: int

    class Congif:
        orm_mode=True

@psqlApp.get('/company_summary', response_model=List[TripSchema])
def get_company_summary(db: Session = Depends(get_db)):
    return    db.query(TaxiModel.company,
                       func.count(TaxiModel.trip_id).label('total_trips'), 
                       func.sum(TaxiModel.trip_total).label('total_fare')).\
                 group_by(TaxiModel.company).all()
