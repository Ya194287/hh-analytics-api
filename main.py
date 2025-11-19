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
            port=url.port or 5432
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
        raw = row['result']
        # Поддержка старых записей (dict) и новых (str)
        if isinstance(raw, dict):
            result = raw
        else:
            result = json.loads(raw)
        result["cached"] = True
        cur.close()
        return result

    # Новый парсинг
    data = parse_hh_sync(query)

    # Аналитика
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

    # Сохраняем как строку (чтобы новые записи были совместимы)
    cur.execute(
        "INSERT INTO analytics (query, result) VALUES (%s, %s) ON CONFLICT (query) DO UPDATE SET result = %s, created_at = NOW()",
        (query.lower(), Json(json.dumps(result)), Json(json.dumps(result)))
    )
    db.commit()
    cur.close()
    return result

def parse_hh_sync(query: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9"
    }
    vacancies = []
    count = 0

    for page in range(0, 2):  # 2 страницы = до 40 вакансий
        time.sleep(1.2 + random.uniform(0, 0.4))
        url = f"https://hh.ru/search/vacancy?text={query}&area=1&page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, 'lxml')

            # Актуальные селекторы на ноябрь 2025
            cards = soup.find_all('div', {'data-qa': 'vacancy-serp__vacancy'}) or \
                    soup.find_all('div', class_=re.compile(r'vacancy-serp-item'))

            for card in cards:
                title_tag = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'}) or \
                           card.find('a', class_=re.compile(r'bloko-link'))
                salary_tag = card.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})

                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                salary_text = salary_tag.get_text(strip=True) if salary_tag else "N/A"
                salary_parsed = parse_salary(salary_text)

                vacancies.append({
                    "title": title,
                    "salary": salary_text,
                    "salary_parsed": salary_parsed
                })
                count += 1
        except Exception:
            break

    return {"count": count, "vacancies": vacancies}

def parse_salary(text: str) -> float:
    if not text or "по договорённости" in text.lower() in text.lower():
        return 0
    text = text.replace(' ', '').replace(' ', '')
    match = re.search(r'(\d+)', text)
    if not match:
        return 0
    num = int(match.group(1))
    if 'тыс' in text.lower():
        return num * 1000
    if 'usd' in text.lower() or '$' in text:
        return num * 90  # курс ~90 на 2025
    return num

# Запуск для локального теста
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
