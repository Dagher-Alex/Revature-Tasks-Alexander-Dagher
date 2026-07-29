from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="Event Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class Event(BaseModel):
    message: str


@app.post("/event/order")
async def order_event(event: Event):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] ORDER EVENT: {event.message}")
    return {"status": "logged", "type": "order", "message": event.message}


@app.post("/event/navigation")
async def navigation_event(event: Event):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] NAVIGATION EVENT: {event.message}")
    return {"status": "logged", "type": "navigation", "message": event.message}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
