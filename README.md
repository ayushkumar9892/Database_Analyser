# Database Analyzer – Backend + Frontend

This project wraps your existing `database_analyser.py` backend logic with a FastAPI server and provides a modern React (Vite + TypeScript + Material UI) frontend to interact with it.

## Prerequisites

- Python 3.10+
- Node.js 18+ and npm
- For SQL Server: ODBC Driver 17 for SQL Server installed on your machine

## 1) Backend setup

1. Create and activate a virtual environment (recommended):
   ```bash
   cd /workspace
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install backend dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the API server:
   ```bash
   uvicorn server:app --host 0.0.0.0 --port 8000 --reload
   ```

The API exposes endpoints such as `/connect`, `/overview`, `/tables`, `/views`, `/table/details`, `/table/indexes`, etc.

## 2) Frontend setup

1. Install frontend dependencies:
   ```bash
   cd /workspace/frontend
   npm install
   ```

2. Start the dev server:
   ```bash
   npm run dev
   ```

The frontend is configured to proxy `/api` requests to `http://localhost:8000`.

## 3) Usage flow

1. Open the frontend in your browser (printed URL from `npm run dev`).
2. Go to the Connect page and enter your database credentials.
3. After connecting, navigate to Overview, Tables, and Views to explore the database.

## Notes

- The FastAPI layer reuses your existing `DatabaseAnalyzer` methods and returns structured JSON for the UI.
- Some heavy operations (e.g., duplicate detection) are summarized for performance.
