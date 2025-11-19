from fastapi import FastAPI, HTTPException
import os
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import pandas as pd
import re

app = FastAPI(title="HH Analytics API @GuglPriv98786")

SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY")

cache = {}  # простой кэш в памяти

@app.get("/")
async def root():
    return {"message": "HH Analytics API работает!", "user": "@GuglPriv98786"}

@app.get("/analytics/{query}")
async def analytics(query: str):
    q = query.lower()
    if q in cache:
        result = cache[q].copy()
        result["cached"] = True
        return result

    api_url = "https://api.scrapingant.com/v2/general"
    params = {
        "url": f"https://hh.ru/search/vacancy?text={query}&area=1",
        "x-api-key": SCRAPINGANT_KEY,
        "browser": "true"   # обязательно — рендерит JS и Cloudflare
    }

    try:
        r = requests.get(api_url, params=params, timeout=40)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')

        cards = soup.find_all('div', {'data-qa': 'vacancy-serp__vacancy'})
        vacancies = []

        for card in cards:
            title_tag = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'})
            salary_tag = card.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})
            title = title_tag.get_text(strip=True) if title_tag else "Без названия"
            salary = salary_tag.get_text(strip=True) if salary_tag else "Не указана"
            salary_num = parse_salary(salary)
            vacancies.append({"title": title, "salary": salary, "salary_parsed": salary_num})

        avg = pd.DataFrame(vacancies)['salary_parsed'].mean()
        avg_str = f"{int(avg):,} ₽" if avg > 0 else "Не указано"

        result = {
            "query": query,
            "count": len(vacancies),
            "avg_salary": avg_str,
            "sample": vacancies[:5],
            "source": "hh.ru (ScrapingAnt)",
            "updated": datetime.now().isoformat(),
            "cached": False
        }
        cache[q] = result
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Парсинг не удался: {str(e)}")

def parse_salary(text: str) -> float:
    if not text or "по договорённости" in text.lower():
        return 0
    text = text.replace(' ', '').replace(' ', '')
    match = re.search(r'(\d+)', text)
    if not match:
        return 0
    num = int(match.group(1))
    return num * 1000 if 'тыс' in text.lower() else num
