from fastapi import FastAPI, HTTPException
import asyncpg
import os
import asyncio
from playwright.async_api import async_playwright
import json
import random
from datetime import datetime
import pandas as pd

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

        data = await parse_hh(query)
        
        # Аналитика Pandas
        df = pd.DataFrame(data['vacancies'])
        avg_salary = df['salary_parsed'].mean() if not df.empty and 'salary_parsed' in df.columns else 0
        geo_count = df['geo'].value_counts().to_dict() if 'geo' in df.columns else {}
        
        result = {
            "query": query,
            "count": data['count'],
            "avg_salary": f"{int(avg_salary):, } ₽" if avg_salary else "N/A",
            "geo_distribution": geo_count,
            "sample": data['vacancies'][:5],
            "source": "hh.ru",
            "updated": datetime.now().isoformat()
        }
        
        await conn.execute(
            "INSERT INTO analytics (query, result) VALUES ($1, $2) ON CONFLICT (query) DO UPDATE SET result = $2, created_at = NOW()",
            query.lower(), json.dumps(result)
        )
        result["cached"] = False
        return result

async def parse_hh(query: str):
    vacancies = []
    count = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "hh-analytics-bot/1.0 (+hi@yourapp.com)"
        })
        
        for page_num in range(0, 2):  # 2 страницы
            await asyncio.sleep(1.1 + random.uniform(0, 0.5))
            await page.goto(f"https://hh.ru/search/vacancy?text={query}&area=1&page={page_num}", wait_until="networkidle")
            
            cards = await page.query_selector_all('.vacancy-serp-item')
            for card in cards:
                title_elem = await card.query_selector('a[data-qa="vacancy-serp__vacancy-title"]')
                salary_elem = await card.query_selector('[data-qa="vacancy-serp__vacancy-compensation"]')
                company_elem = await card.query_selector('[data-qa="vacancy-serp__vacancy-employer"]')
                
                title = await title_elem.inner_text() if title_elem else "N/A"
                salary_text = await salary_elem.inner_text() if salary_elem else "N/A"
                company = await company_elem.inner_text() if company_elem else "N/A"
                
                # Парсинг зарплаты (пример)
                salary_parsed = parse_salary(salary_text)
                
                # Время жизни (пример, из date)
                life_time = random.randint(7, 30)  # Заглушка, парси реальную date
                
                vacancies.append({
                    "title": title.strip(),
                    "salary": salary_text.strip(),
                    "salary_parsed": salary_parsed,
                    "company": company.strip(),
                    "geo": "Москва",  # Из URL area=1
                    "life_time_days": life_time
                })
                count += 1
        
        await browser.close()
    
    return {"count": count, "vacancies": vacancies}

def parse_salary(text: str) -> float:
    import re
    match = re.search(r'от\s*(\d+(?:\.\d+)?)', text.replace(' ', ''))
    return float(match.group(1)) * 1000 if match else 0
