from datetime import datetime
from shapely import Point
from pydantic import BaseModel

class Trip(BaseModel):
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
    
class CommunityArea(BaseModel):
    community_area_id: int
    community_area_name: str
    community_area_size: int

    class Congif:
        orm_mode=True