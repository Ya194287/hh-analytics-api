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
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    try:
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

    except Exception as e:
        logger.error(f"Ошибка в /analytics/{query}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

def parse_hh_sync(query: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    vacancies = []
    count = 0

    for page in range(2):
        time.sleep(1.2 + random.uniform(0, 0.4))
        url = f"https://hh.ru/search/vacancy?text={query}&area=1&page={page}"
        try:
            logger.info(f"Парсим: {url}")
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                logger.warning(f"Статус {r.status_code} для {url}")
                break

            soup = BeautifulSoup(r.text, 'lxml')

            # Новые селекторы (на ноябрь 2025)
            cards = soup.find_all('div', {'data-qa': 'vacancy-serp__vacancy'})
            if not cards:
                cards = soup.find_all('div', class_=re.compile(r'vacancy-serp-item'))

            for card in cards:
                title_tag = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'}) or \
                           card.find('a', class_=re.compile(r'bloko-link'))
                salary_tag = card.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})

                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                salary = salary_tag.get_text(strip=True) if salary_tag else "N/A"
                parsed = parse_salary(salary)

                vacancies.append({
                    "title": title,
                    "salary": salary,
                    "salary_parsed": parsed
                })
                count += 1

        except Exception as e:
            logger.error(f"Ошибка парсинга страницы {page}: {str(e)}")
            break

    return {"count": count, "vacancies": vacancies}

def parse_salary(text: str) -> float:
    if not text or text == "N/A" or "по договорённости" in text:
        return 0
    text = text.replace(' ', '').replace(' ', '')
    match = re.search(r'(\d+)', text)
    if not match:
        return 0
    num = int(match.group(1))
    if 'тыс' in text:
        return num * 1000
    if 'USD' in text or '$' in text:
        return num * 90  # курс на 2025
    return num
