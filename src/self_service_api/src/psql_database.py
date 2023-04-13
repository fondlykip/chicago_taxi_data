from sqlalchemy import create_engine
from sqlalchemy.sql.expression import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, Depends
import os
from typing import List
from sqlalchemy import (Column,
                        ForeignKey,
                        Integer,
                        String,
                        DateTime,
                        Float,
                        Time)
from geoalchemy2 import Geometry
from pydantic import BaseModel, Field

PSQL_TRIP_DB_CONN_STR="postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips"

engine = create_engine(os.environ["PSQL_CONN_STR"])
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

psqlBase = declarative_base()

psqlApp = FastAPI()

def get_db():
    db = Session(engine)
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
    

class CompanySummarySchema(BaseModel):
    company: str#  = Field(...)
    total_trips: int#  = Field(...)
    total_fare: float#  = Field(...)

    class Config:
        orm_mode=True

@psqlApp.get('/company_summary', response_model=List[CompanySummarySchema])
def get_company_summary(db: Session = Depends(get_db)):
    return    db.query(TaxiModel.company,
                       func.count(TaxiModel.trip_id).label('total_trips'), 
                       func.sum(TaxiModel.trip_total).label('total_fare')).\
                 group_by(TaxiModel.company).all()
