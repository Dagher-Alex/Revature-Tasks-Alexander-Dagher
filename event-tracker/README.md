# Event Tracker — Hackathon Project

A minimal frontend + FastAPI backend that fires and logs events.

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the backend
```bash
python main.py
# or: uvicorn main:app --reload
```
Server starts at **http://localhost:8000**

### 3. Open the frontend
Just open `index.html` in your browser — no build step needed.

## Endpoints

| Method | Path | Body |
|--------|------|------|
| POST | `/event/order` | `{ "message": "..." }` |
| POST | `/event/navigation` | `{ "message": "..." }` |

Both endpoints print a timestamped log line to the terminal and return JSON confirmation.

## Project Structure
```
event-tracker/
├── main.py           # FastAPI backend
├── index.html        # Frontend (two buttons)
├── requirements.txt
└── README.md
```
