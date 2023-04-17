"""
main file of the API code specifies the main entry point for the API
and mounts to code for the mongo and psql query APIs
"""
from enum import Enum
from fastapi import FastAPI
from fastapi.responses import FileResponse
from .psql_database import psqlApp
from .mongo_database import mongoApp


APP = FastAPI()
APP.mount("/api/psql", psqlApp)
APP.mount("/api/mongo", mongoApp)

class DatasetNames(str, Enum):
    "Model for validating API input Dataset Names"
    mongo = 'mongo'
    psql = 'psql'

class FormatNames(str, Enum):
    "Model for validating API input Format Names"
    csv = 'csv'
    json = 'json'

@APP.get("/dump/{dataset}/{format_name}", response_class=FileResponse)
def get_dump(dataset: DatasetNames, format_name: FormatNames):
    "Return bulk export for a given dataset in a given format"
    file_path = f"/remote-storage/dumps/{dataset}/trip_dump.{format_name}"
    response = FileResponse(file_path, media_type="text/csv")
    content_disp_str = f"attachment; filename={dataset}_trip_dump.{format_name}"
    response.headers["Content-Disposition"] = content_disp_str
    return response
