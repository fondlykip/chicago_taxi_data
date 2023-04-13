from sqlalchemy import create_engine
from sqlalchemy.sql.expression import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from . import psql_models, psql_schemas
from fastapi import FastAPI
import os
from typing import Iterator, Any
from fastapi_pagination import LimitOffsetPage, Page, add_pagination
from fastapi_pagination.ext.sqlalchemy import paginate

PSQL_TRIP_DB_CONN_STR="postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips"

engine = create_engine(os.environ["PSQL_CONN_STR"])
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

psqlBase = declarative_base()

psqlApp = FastAPI()

def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@psqlApp.get('/trips/csv')
def get_trips_as_csv(db: Session, ):
    db.query(psql_models.TaxiTrips)
    return

@psqlApp.get('/trips/json')
def get_trips_as_json():
    return


def get_items(db: Session, skip: int = 0, limit: int = 100):
    return db.query(psql_models.TaxiTrips).offset(skip).limit(limit).all()

@psqlApp.get
def get_company_summary(db: Session):
    db.query(func.count(psql_models.TaxiTrips.trip_id).label('total_trips'), 
             func.sum(psql_models.TaxiTrips.trip_total).label('total_fare')).\
        group_by(psql_models.TaxiTrips.company)