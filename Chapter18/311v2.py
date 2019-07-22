from fastapi import FastAPI, Depends
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
from enum import Enum
import json
import joblib
import webpage
from ml import TimeTransformer
import logging

logger = logging.getLogger("LOGGER")


class ComplaintType(str, Enum):
    other = "other"
    commercial = "commercial"
    park = "park"
    residential = "residential"
    street = "street"
    vehicle = "vehicle"
    worship = "worship"
    truck = "truck"


class naive_model:
    data = None

    def __init__(self):
        path = "./model.json"

        with open(path, "r") as f:
            self.data = json.load(f)

    def predict(self, type_):
        return self.data.get(type_, None)


model = naive_model()

app = FastAPI(
    title="311 toy service api",
    description="311 toy service API for Packt Python 3.7 book",
    version="0.1.0",
)


@app.get("/complaints/all/{complaint_type}/time")
def complaints(complaint_type: str):
    return {
        "complaint_type": complaint_type,
        "expected_time": model.predict(complaint_type),
    }


@app.get("/complaints/noise/{complaint_type}/time")
def complaints(complaint_type: ComplaintType):
    if complaint_type == ComplaintType.other:
        ct = "noise"
    else:
        ct = f"noise - {complaint_type.value}" 

    return {
        "complaint_type": complaint_type,
        "ct": ct,
        "expected_time": model.predict(ct),
    }


class Complaint(BaseModel):
    complaint_type: ComplaintType
    timestamp: datetime = datetime.now()
    lat: float
    lon: float
    description: str


@app.post("/input/", tags=["input"])
def enter_complaint(body: Complaint):
    print(body.dict())
    return body.dict()


clf = joblib.load("model.joblib")


@app.get("/predict/{complaint_type}", tags=["predict"])
def predict_time(
    complaint_type: ComplaintType,
    latitude: float,
    longitude: float,
    created_date: datetime,
):

    obj = pd.DataFrame(
        [
            {
                "complaint_type": complaint_type.value,
                "latitude": latitude,
                "longitude": longitude,
                "created_date": created_date,
            },
        ]
    )

    logger.info(obj.dtypes.to_string())
    logger.info(obj.to_string())

    predicted = clf.predict(obj[['complaint_type', 'latitude', 'longitude','created_date']])
    logger.info(predicted)
    return {"estimated_time": predicted[0]}


@app.get("/predict_async/{complaint_type}", tags=["predict"])
def predict_time_async(
    complaint_type: ComplaintType,
    latitude: float,
    longitude: float,
    created_date: datetime,
):

    obj = pd.DataFrame(
        [
            {
                "complaint_type": complaint_type.value,
                "latitude": latitude,
                "longitude": longitude,
                "created_date": created_date,
            }
        ]
    )
    obj = obj[["complaint_type", "latitude", "longitude", "created_date"]]

    predicted = clf.predict(obj)
    logger.info(predicted)
    return {"estimated_time": predicted[0]}


app.include_router(webpage.router, prefix="/dashboard")
