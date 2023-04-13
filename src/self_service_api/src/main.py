from fastapi import FastAPI
from fastapi.responses import FileResponse
from .psql_database import psqlApp
from .mongo_database import mongoApp
from enum import Enum

app = FastAPI()
app.mount("/api/psql", psqlApp)
app.mount("/api/mongo", mongoApp)

class DatasetNames(str, Enum):
    mongo = 'mongo'
    psql = 'psql'

class FormatNames(str, Enum):
    csv = 'csv'
    json = 'json'

@app.get("/dump/{dataset}/{format}", response_class=FileResponse)
def get_dump(dataset: DatasetNames, format: FormatNames):
    file_path = f"/remote-storage/dumps/{dataset}/trip_dump.{format}"
    response = FileResponse(file_path, media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={dataset}_trip_dump.{format}"
    return response