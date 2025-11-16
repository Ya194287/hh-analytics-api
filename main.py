from fastapi import FastAPI, HTTPException
import os
import asyncio
from playwright.async_api import async_playwright
import json
import random
from datetime import datetime
import asyncpg  # Удалим, используем psycopg3
from psycopg_pool import AsyncConnectionPool
from psycopg import AsyncConnection

app = FastAPI(title="HH Analytics API @GuglPriv98786")

POSTGRES_URL = os.getenv("POSTGRES_URL")
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = AsyncConnectionPool(POSTGRES_URL, min_size=1, max_size=10)
    async with pool.connection() as conn:
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
    async with pool.connection() as conn:
        row = await conn.fetchrow("SELECT result FROM analytics WHERE query = $1", query.lower())
        if row:
            result = json.loads(row["result"])
            result["cached"] = True
            return result

        data = await parse_hh(query)
        await conn.execute(
            "INSERT INTO analytics (query, result) VALUES ($1, $2) ON CONFLICT (query) DO UPDATE SET result = $2, created_at = NOW()",
            query.lower(), json.dumps(data)
        )
        data["cached"] = False
        return data

async def parse_hh(query: str):
    await asyncio.sleep(1.1 + random.uniform(0, 0.5))  # Rate limit <1 req/sec
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
            vacancies.append({"title": title.strip(), "salary": salary.strip()})
        
        await browser.close()
        return {
            "query": query,
            "count": len(vacancies),
            "sample": vacancies[:5],
            "source": "hh.ru",
            "region": "Москва",
            "updated": datetime.now().isoformat()
        }
