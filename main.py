from fastapi import FastAPI
import requests
import json
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus

app = FastAPI(title="HH Analytics API @GuglPriv98786")

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

    # Правильно кодируем запрос
    search_text = quote_plus(query)

    url = "https://api.hh.ru/vacancies"
    params = {
        "text": query,           # человекочитаемый
        "area": 1,               # Москва
        "per_page": 100,         # максимум
        "page": 0,
        "only_with_salary": False
    }
    headers = {"User-Agent": "HH-Analytics-Bot/1.0 (+hi@yourapp.com)"}

    r = requests.get(url, params=params, headers=headers, timeout=30)
    data = r.json()

    vacancies = []
    salaries = []

    for item in data.get("items", []):
        name = item.get("name", "Без названия")

        salary = item.get("salary")
        if salary and salary.get("from"):
            fr = salary["from"]
            to = salary.get("to")
            curr = salary.get("currency", "RUR")

            if to:
                sal_text = f"{fr:,}–{to:,} {curr}".replace(',', ' ')
                avg_val = (fr + to) / 2
            else:
                sal_text = f"от {fr:,} {curr}".replace(',', ' ')
                avg_val = fr
        else:
            sal_text = "Не указана"
            avg_val = 0

        vacancies.append({"title": name, "salary": sal_text})
        if avg_val > 0:
            salaries.append(avg_val)

    avg = int(pd.Series(salaries).mean()) if salaries else 0 else 0
    avg_str = f"{avg:,} ₽" if avg > 0 else "Не указано"

    result = {
        "query": query,
        "count": len(vacancies),
        "avg_salary": avg_str,
        "sample": vacancies[:5],
        "source": "hh.ru official API",
        "updated": datetime.now().isoformat(),
        "cached": False
    }

    cache[q] = result
    return result
