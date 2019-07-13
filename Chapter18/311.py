from fastapi import FastAPI
import json


class naive_model:
    data = None

    def __init__(self, path="./model.json"):

        with open(path, "r") as f:
            self.data = json.load(f)

    def predict(self, type_):
        return self.data.get(type_, None)


app = FastAPI()
model = naive_model()


@app.get("/complaints/time/{complaint_type}")
def complaints(complaint_type: str):
    return {
        "complaint_type": complaint_type,
        "expected_time": model.predict(complaint_type),
    }
