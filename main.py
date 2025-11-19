from fastapi import FastAPI, HTTPException
import os
import requests
import json
from datetime import datetime
import pandas as pd

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

    api_url = "https://api.hh.ru/vacancies"
    params = {
        "text": query,
        "area": 1,  # Москва
        "per_page": 50,  # до 50 вакансий
        "page": 0
    }
    headers = {"User-Agent": "HH-Analytics-Bot/1.0 (+hi@yourapp.com)"}

    try:
        r = requests.get(api_url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()

        vacancies = []
        for item in data.get("items", []):
            name = item.get("name", "Без названия")
            salary = item.get("salary")
            if salary:
                from_sal = salary.get("from")
                to_sal = salary.get("to")
                currency = salary.get("currency", "RUR")
                sal_text = f"от {from_sal}" if from_sal and not to_sal else f"{from_sal}–{to_sal}" if from_sal and to_sal else f"до {to_sal}"
                sal_text += f" {currency}"
                sal_parsed = (from_sal or to_sal or 0)
            else:
                sal_text = "Не указана"
                sal_parsed = 0

            vacancies.append({"title": name, "salary": sal_text, "salary_parsed": sal_parsed})

        df = pd.DataFrame(vacancies)
        avg = df['salary_parsed'].mean()
        avg_str = f"{int(avg):,} ₽" if avg > 0 else "Не указано"

        result = {
            "query": query,
            "count": len(vacancies),
            "avg_salary": avg_str,
            "sample": vacancies[:5],
            "source": "hh.ru (official API)",
            "updated": datetime.now().isoformat(),
            "cached": False
        }
        cache[q] = result
        return result

    except Exception as e:
        raise HTTPException(500, f"Ошибка API: {str(e)}")
