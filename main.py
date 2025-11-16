from fastapi import FastAPI, HTTPException
import asyncpg
import os
import asyncio
import requests
from bs4 import BeautifulSoup
import json
import random
from datetime import datetime
import pandas as pd
import re

app = FastAPI(title="HH Analytics API @GuglPriv98786")

POSTGRES_URL = os.getenv("POSTGRES_URL")
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(POSTGRES_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                query TEXT UNIQUE,
                result JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

@app.get("/")
async def root():
    return {"message": "HH Analytics API работает!", "time": datetime.now().isoformat(), "user": "@GuglPriv98786"}

@app.get("/analytics/{query}")
async def get_analytics(query: str):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT result FROM analytics WHERE query = $1", query.lower())
        if row:
            result = json.loads(row['result'])
            result["cached"] = True
            return result

        data = await parse_hh_requests(query)
        
        # Аналитика
        df = pd.DataFrame(data['vacancies'])
        salaries = [s for s in df['salary_parsed'] if s > 0]
        avg_salary = sum(salaries) / len(salaries) if salaries else 0
        
        result = {
            "query": query,
            "count": data['count'],
            "avg_salary": f"{int(avg_salary):,} ₽" if avg_salary else "N/A",
            "sample": data['vacancies'][:5],
            "source": "hh.ru (requests+BS4)",
            "updated": datetime.now().isoformat(),
            "cached": False
        }
        
        await conn.execute(
            "INSERT INTO analytics (query, result) VALUES ($1, $2) ON CONFLICT (query) DO UPDATE SET result = $2",
            query.lower(), json.dumps(result)
        )
        return result

async def parse_hh_requests(query: str):
    headers = {
        "User-Agent": "hh-analytics-bot/1.0 (+hi@yourapp.com)",
        "Accept-Language": "ru-RU,ru;q=0.9"
    }
    vacancies = []
    count = 0
    
    for page in range(2):
        await asyncio.sleep(1.1 + random.uniform(0, 0.5))
        url = f"https://hh.ru/search/vacancy?text={query}&area=1&page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                break
            soup = BeautifulSoup(response.text, 'lxml')
            cards = soup.find_all('div', class_='vacancy-serp-item__layout')
            
            for card in cards:
                title_tag = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'})
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
        except:
            break
    
    return {"count": count, "vacancies": vacancies}

def parse_salary(text: str) -> float:
    if not text or text == "N/A":
        return 0
    match = re.search(r'(\d+[\d\s]*)', text.replace(' ', ''))
    if not match:
        return 0
    num = int(''.join(filter(str.isdigit, match.group(1))))
    return num * 1000 if 'тыс' in text.lower() else num
