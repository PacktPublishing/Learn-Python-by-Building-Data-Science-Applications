from fastapi import FastAPI

app = FastAPI()

db = {"noise": 24, "broken hydrant": 2}


@app.get("/complaints/{complaint_type}")
def complaints(complaint_type: str, hour: int):
    return {
        "complaint_type": complaint_type,
        "hour": hour,
        "q": db.get(complaint_type, None),
    }
