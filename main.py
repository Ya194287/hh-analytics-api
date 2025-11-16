from fastapi import FastAPI, HTTPException
import asyncpg
import os
import asyncio
from playwright.async_api import async_playwright
import json
import random
from datetime import datetime

app = FastAPI(title="HH Analytics API @GuglPriv98786")

POSTGRES_URL = os.getenv("POSTGRES_URL")
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(POSTGRES_URL)
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS analytics (
            id SERIAL PRIMARY KEY,
            query TEXT UNIQUE,
            result JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

@app.get("/")
async def root():
    return {"message": "HH Analytics API работает!", "time": datetime.now().isoformat()}

@app.get("/analytics/{query}")
async def get_analytics(query: str):
    try:
        # Попробуем из БД
        row = await pool.fetchrow("SELECT result FROM analytics WHERE query = $1", query.lower())
        if row:
            return json.loads(row["result"])

        # Парсим
        data = await parse_hh(query)
        await pool.execute(
            "INSERT INTO analytics (query, result) VALUES ($1, $2) ON CONFLICT (query) DO UPDATE SET result = $2, created_at = NOW()",
            query.lower(), json.dumps(data)
        )
        return data
    except Exception as e:
        raise HTTPException(500, str(e))

async def parse_hh(query: str):
    await asyncio.sleep(1.1 + random.uniform(0, 0.5))  # <1 req/sec
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "hh-analytics-bot/1.0 (+hi@yourapp.com)"
        })
        await page.goto(f"https://hh.ru/search/vacancy?text={query}&area=1", wait_until="networkidle", timeout=60000)
        
        vacancies = []
        cards = await page.query_selector_all('.vacancy-serp-item')
        for card in cards[:20]:
            title_elem = await card.query_selector('a[data-qa="vacancy-serp__vacancy-title"]')
            salary_elem = await card.query_selector('[data-qa="vacancy-serp__vacancy-compensation"]')
            title = await title_elem.inner_text() if title_elem else "N/A"
            salary = await salary_elem.inner_text() if salary_elem else "N/A"
            vacancies.append({"title": title, "salary": salary})
        
        await browser.close()
        return {
            "query": query,
            "count": len(vacancies),
            "sample": vacancies[:5],
            "source": "hh.ru",
            "cached": False
        }
