""" Frontend API generator """

from fastapi import FastAPI
from core.method_generator import AutoDB
from service_definitions.registry import TASKS

db = AutoDB()
app = FastAPI()


@app.post("/api/{service}/handle")
async def handle_service(service: str, payload: dict):
    """ Service handler endpoint """

    if service not in TASKS:
        return {"error": "Unknown service"}
    table = f"{service}_requests"
    user_id = payload["user_id"]
    text = payload.get("text", "")
    db.execute(f"INSERT INTO {table} (user_id, text, status) VALUES (?, ?, ?)",
               (user_id, text, "pending"))
    return {"message": "Accepted", "user_id": user_id}


@app.get("/api/{service}/status/{user_id}")
def get_status(service: str, user_id: int):
    """ User request status endpoint """

    table = f"{service}_requests"
    rows = db.execute(f"SELECT status, image_url FROM {table} WHERE user_id=?", (user_id,))
    return {"results": rows}
