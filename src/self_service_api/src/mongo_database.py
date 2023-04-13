import os
from fastapi import FastAPI
from pydantic import BaseModel, Field
from bson import ObjectId
from geojson_pydantic.geometries import Point
from typing import List
import datetime
import motor.motor_asyncio

mongoApp = FastAPI()
client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGO_CONN_STR"])
db = client.chicago_taxi_trips_database

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")
    
class TripModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    taxi_id: str = Field(...)
    trip_start_timestamp: datetime.datetime = Field(...)
    trip_end_timestamp: datetime.datetime = Field(...)
    trip_seconds: int = Field(...)
    trip_miles: float = Field(...)
    pickup_census_tract: str = Field(...)
    dropoff_census_tract: str = Field(...)
    pickup_community_area: int = Field(...)
    dropoff_community_area: int = Field(...)
    fare: float = Field(...)
    tips: float = Field(...)
    tolls: float = Field(...)
    extras: float = Field(...)
    trip_total: float = Field(...)
    payment_type: str = Field(...)
    company: str = Field(...)
    pickup_centroid_latitude: float = Field(...)
    pickup_centroid_longitude: float = Field(...)
    pickup_centroid_location: Point = Field(...)
    dropoff_centroid_latitude: float = Field(...)
    dropoff_centroid_longitude: float = Field(...)
    dropoff_centroid_location: Point = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class CompanySummaryModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    total_trips: int = Field(...)
    total_fare: float = Field(...)
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

@mongoApp.get('/company_summary', 
              response_model=List[CompanySummaryModel])
async def return_trips_as_csv():
    pipeline = [{"$group": 
                        {"_id": "$company",
                         "total_trips": {"$sum": 1},
                         "total_fair": {"$sum": "$total_fare"}}
                }]
    companies = await db.chicago_taxi_trips_collection.aggregate(pipeline).to_list(1000)
    return companies

