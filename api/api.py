from typing import List
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from bson import ObjectId
import yaml
import motor.motor_asyncio


app = FastAPI()

with open("../config.yaml", 'r') as cfg:
    config = yaml.safe_load(cfg)


# Connect to MongoDB
    
conn_url = config['connection_url']
client = motor.motor_asyncio.AsyncIOMotorClient(conn_url)
#client = MongoClient(conn_url)

db = client[config['database']]
delivery_collection = db[config['collection']]


class DeliveryTrip(BaseModel):
    vehicle_no: str
    GpsProvider: str
    BookingID: str
    
    Origin_Location: str
    Destination_Location: str
    Current_Location: str

    BookingID_Date: str
    trip_start_date: str
    trip_end_date: str
    Planned_ETA: str


@app.get("/vehicle_no/", response_description="List all vehicle numbers", response_model=List[DeliveryTrip])
async def list_delivery_trips():
    try:
        # MongoDB query to retrieve all documents and only the 'vehicle_no' field
        vehicle_no = await delivery_collection.find({}, {"vehicle_no": 1}).to_list(length=None)

        # Create a list of DeliveryTrip objects
        delivery_trip_objects = [DeliveryTrip(vehicle_no=trip["vehicle_no"]) for trip in vehicle_no]
        
        return delivery_trip_objects
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/delivery_trips/{vehicle_no}", response_description="Get details for a specific vehicle_no", response_model=DeliveryTrip)
async def get_delivery_trip(vehicle_no: str):
    try:
        # MongoDB query to retrieve details for a specific vehicle_no
        delivery_trip = await delivery_collection.find_one({"vehicle_no": vehicle_no})

        # Check if the vehicle_no exists in the collection
        if not delivery_trip:
            raise HTTPException(status_code=404, detail="Vehicle not found")

        return DeliveryTrip(**delivery_trip)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

class GpsProviderCount(BaseModel):
    gps_provider: str
    vehicle_count: int


@app.get("/vehicle_count_by_gps_provider/", response_description="Count vehicle_no grouped by GpsProvider", response_model=List[GpsProviderCount])
async def count_vehicle_by_gps_provider():
    try:
        # MongoDB aggregation pipeline to count vehicle_no grouped by GpsProvider
        pipeline = [
        {"$group": {"_id": "$GpsProvider", "vehicle_count": {"$sum": 1}}},
        {"$project": {"gps_provider": "$_id", "vehicle_count": 1, "_id": 0}}
    ]


        result = await delivery_collection.aggregate(pipeline).to_list(length=None)

        # Check if any documents were found
        if not result:
            raise HTTPException(status_code=404, detail="No data found for counting vehicle_no by GpsProvider")

        return [GpsProviderCount(**item) for item in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))