from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

PSQL_TRIP_DB_CONN_STR="postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips"

engine = create_engine(PSQL_TRIP_DB_CONN_STR)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()