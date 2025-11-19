from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import os
import requests
from bs4 import BeautifulSoup
import json
import random
import time
from datetime import datetime
import pandas as pd
import re

app = FastAPI(title="HH Analytics API @GuglPriv98786")

POSTGRES_URL = os.getenv("POSTGRES_URL")  # у тебя уже есть

conn = None

def get_db():
    global conn
    if conn is None or conn.closed:
        import urllib.parse as up
        up.uses_netloc.append('postgres')
        url = up.urlparse(POSTGRES_URL)
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                query TEXT UNIQUE,
                result JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
    return conn

@app.get("/")
def root():
    return {"message": "HH Analytics API работает!", "time": datetime.now().isoformat()}

@app.get("/analytics/{query}")
def get_analytics(query: str):
    db = get_db()
    cur = db.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT result FROM analytics WHERE query = %s", (query.lower(),))
    row = cur.fetchone()
    if row:
        result = json.loads(row['result'])
        result["cached"] = True
        cur.close()
        return result

    data = parse_hh_sync(query)
    df = pd.DataFrame(data['vacancies'])
    salaries = [s for s in df['salary_parsed'] if s > 0]
    avg_salary = sum(salaries) / len(salaries) if salaries else 0

    result = {
        "query": query,
        "count": data['count'],
        "avg_salary": f"{int(avg_salary):,} ₽" if avg_salary else "N/A",
        "sample": data['vacancies'][:5],
        "source": "hh.ru",
        "updated": datetime.now().isoformat(),
        "cached": False
    }

    # корректировка:
    cur.execute(
        "INSERT INTO analytics (query, result) VALUES (%s, %s) ON CONFLICT (query) DO UPDATE SET result = %s",
        (query.lower(), Json(json.dumps(result)), Json(json.dumps(result)))
    )
    db.commit()
    cur.close()
    return result

# parse_hh_sync и parse_salary — оставляем как есть (они работают)

# ... (остальной код без изменений)
