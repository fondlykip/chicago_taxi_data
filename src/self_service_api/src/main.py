from typing import Union
from fastapi import FastAPI
from .psql_database import psqlBase, psqlApp
from .mongo_database import mongoApp

app = FastAPI()
app.mount("/api/psql", psqlApp)
app.mount("/api/mongo", mongoApp)


@app.get("/")
def read_root():
    return "See localhost:9001/docs for more info"

