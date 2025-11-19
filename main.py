from fastapi import FastAPI, HTTPException
import os
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO)

app = FastAPI()

SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY")
if not SCRAPINGANT_KEY:
    raise Exception("Добавь SCRAPINGANT_KEY в Environment на Render")

cache = {}

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

    vacancies = []
    url = f"https://api.scrapingant.com/v2/general?url=https%3A%2F%2Fhh.ru%2Fsearch%2Fvacancy%3Ftext%3D{query}%26area%3D1&x-api-key={SCRAPINGANT_KEY}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        cards = soup.find_all('div', {'data-qa': 'vacancy-serp__vacancy'})

        for card in cards:
            title_tag = card.find('a', {'data-qa': 'vacancy-serp__vacancy-title'})
            salary_tag = card.find('span', {'data-qa': 'vacancy-serp__vacancy-compensation'})
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            salary = salary_tag.get_text(strip=True) if salary_tag else "N/A"
            salary_num = parse_salary(salary)
            vacancies.append({"title": title, "salary": salary, "salary_parsed": salary_num})

        avg = pd.DataFrame(vacancies)['salary_parsed'].mean()
        avg_str = f"{int(avg):,} ₽" if avg > 0 else "N/A"

        result = {
            "query": query,
            "count": len(vacancies),
            "avg_salary": avg_str,
            "sample": vacancies[:5],
            "source": "hh.ru via ScrapingAnt",
            "updated": datetime.now().isoformat(),
            "cached": False
        }
        cache[q] = result
        return result

    except Exception as e:
        logging.error(str(e))
        raise HTTPException(500, "Парсинг временно недоступен")

def parse_salary(t: str) -> float:
    if not t or "по договорённости" in t: return 0
    t = t.replace(' ', '').replace(' ', '')
    m = re.search(r'(\d+)', t)
    if not m: return 0
    num = int(m.group(1))
    return num * 1000 if 'тыс' in t.lower() else num
