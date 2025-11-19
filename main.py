from fastapi import FastAPI, HTTPException
import requests
import json
from datetime import datetime
import pandas as pd

app = FastAPI(title="HH Analytics API @GuglPriv98786")

#")

cache = {}

@app.get("/")
async def root():
    return {"message": "HH Analytics API работает!", "user": "@GuglPriv98786"}

@app.get("/analytics/{query}")
async def analytics(query: str):
    q = query.lower()

    # Кэш
    if q in cache:
        result = cache[q].copy()
        result["cached"] = True
        return result

    # Официальный API hh.ru
    api_url = "https://api.hh.ru/vacancies"
    params = {
        "text": query,
        "area": 1,           # Москва
        "per_page": 50,      # максимум за один запрос
        "only_with_salary": False
    }
    headers = {"User-Agent": "HH-Analytics-Bot/1.0 (+hi@yourapp.com)"}

    try:
        r = requests.get(api_url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()

        vacancies = []
        salaries_for_avg = []

        for item in data.get("items", []):
            name = item.get("name", "Без названия")

            salary_obj = item.get("salary")
            if salary_obj and salary_obj.get("from"):
                from_sal = salary_obj["from"]
                to_sal = salary_obj.get("to")
                currency = salary_obj.get("currency", "RUR")

                if to_sal:
                    sal_text = f"{from_sal}–{to_sal} {currency}"
                    avg_val = (from_sal + to_sal) / 2
                else:
                    sal_text = f"от {from_sal} {currency}"
                    avg_val = from_sal

                # Приводим всё к рублям (примерно)
                if currency == "USD":
                    avg_val *= 90
                elif currency == "EUR":
                    avg_val *= 100

                salaries_for_avg.append(avg_val)
            else:
                sal_text = "Не указана"
                avg_val = 0

            vacancies.append({
                "title": name,
                "salary": sal_text,
                "salary_parsed": avg_val
            })

        # Средняя зарплата
        avg_salary = int(pd.Series(salaries_for_avg).mean()) if salaries_for_avg else 0
        avg_str = f"{avg_salary:,} ₽" if avg_salary > 0 else "Не указано"

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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка API hh.ru: {str(e)}")
