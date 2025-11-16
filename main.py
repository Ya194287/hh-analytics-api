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

POSTGRES_URL = os.getenv("POSTGRES_URL")
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
        cur.close()
    return conn

@app.get("/")
def root():
    return {"message": "HH Analytics API работает!", "time": datetime.now().isoformat(), "user": "@GuglPriv98786"}

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

    cur.execute(
        "INSERT INTO analytics (query, result) VALUES (%s, %s) ON CONFLICT (query) DO UPDATE SET result = %s",
        (query.lower(), Json(result), Json(result))
    )
    db.commit()
    cur.close()
    return result

def parse_hh_sync(query: str):
    headers = {"User-Agent": "hh-analytics-bot/1.0 (+hi@yourapp.com)"}
    vacancies = []
    for page in range(2):
        time.sleep(1.1 + random.uniform(0, 0.5))
        url = f"https://hh.ru/search/vacancy?text={query}&area=1&page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: break
            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.find_all('div', class_='vacancy-serp-item__layout')
            for card in cards:
                t = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'})
                s = card.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})
                title = t.get_text(strip=True) if t else "N/A"
                salary = s.get_text(strip=True) if s else "N/A"
                parsed = parse_salary(salary)
                vacancies.append({"title": title, "salary": salary, "salary_parsed": parsed})
        except: break
    return {"count": len(vacancies), "vacancies": vacancies}

def parse_salary(text: str) -> float:
    if not text or "от" not in text: return 0
    match = re.search(r'(\d+[\d\s]*)', text.replace(' ', ''))
    if not match: return 0
    num = int(''.join(filter(str.isdigit, match.group(1))))
    return num * 1000
