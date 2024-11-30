import asyncio
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
STOCK_COLLECTION_NAME = "stocks"
STOCK_DETAILS_COLLECTION = "stock_data"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
stock_collection = db[STOCK_COLLECTION_NAME]
stock_details_collection = db[STOCK_DETAILS_COLLECTION]

# FastAPI app
app = FastAPI()


# Pydantic Models
class StockList(BaseModel):
    stock_symbols: str  # Comma-separated stock symbols


# Helper Functions
def fetch_page(url: str) -> BeautifulSoup:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        return BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching page: {e}")


def parse_ul_top_ratios(stock_symbol: str, soup: BeautifulSoup):
    ul_element = soup.find("ul", id="top-ratios")
    if not ul_element:
        return {}

    items_dict = {}
    for li in ul_element.find_all("li"):
        name_span = li.find("span", class_="name")
        value_span = li.find("span", class_="value")
        if name_span and value_span:
            items_dict[name_span.get_text(strip=True)] = value_span.get_text(strip=True)

    return {"stock_symbol": stock_symbol, "stock_details": items_dict}


def parse_shareholder_table(stock_symbol: str, soup: BeautifulSoup):
    table = soup.find("div", id="quarterly-shp")
    if not table:
        return {}

    headers = [th.text.strip() for th in table.find("thead").find("tr").find_all("th")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        row_data = {
            headers[idx]: cell.get_text(strip=True)
            for idx, cell in enumerate(tr.find_all("td"))
        }
        rows.append(row_data)

    return {"shareholder": rows}


def parse_peer_comparision_table(stock_symbol: str, soup: BeautifulSoup):
    table = soup.find("div", id="peers-table-placeholder")
    if not table:
        return {}

    headers = [th.text.strip() for th in table.find("thead").find("tr").find_all("th")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        row_data = {
            headers[idx]: cell.get_text(strip=True)
            for idx, cell in enumerate(tr.find_all("td"))
        }
        rows.append(row_data)

    return {"peer_comparision": rows}


@app.post("/scrape-all-data")
async def scrape_shareholder_data(payload: StockList):

    stock_symbols = [
        symbol.strip().upper() for symbol in payload.stock_symbols.split(",")
    ]

    async def scrape_and_save(stock_symbol):
        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            details_data = parse_ul_top_ratios(stock_symbol, soup)
            shareholder_data = parse_shareholder_table(stock_symbol, soup)
            peer_comparision_data = parse_peer_comparision_table(stock_symbol, soup)

            if details_data or shareholder_data or peer_comparision_data:
                combined_data = {
                    "stock_symbol": stock_symbol,
                    **details_data,
                    **shareholder_data,
                    **peer_comparision_data,
                }
                stock_details_collection.insert_one(combined_data)
                return {
                    "stock_symbol": stock_symbol,
                    "message": "Data scraped and saved successfully.",
                }
            return {
                "stock_symbol": stock_symbol,
                "message": "No data found to scrape.",
            }
        except Exception as e:
            return {"stock_symbol": stock_symbol, "error": str(e)}

    results = []
    for symbol in stock_symbols:
        result = await scrape_and_save(symbol)
        results.append(result)
        await asyncio.sleep(5)  

    return {"results": results}


@app.get("/get-stock-symbols")
async def get_stock_symbols():
    try:
        stocks = stock_collection.find({}, {"_id": 0, "Symbol": 1})
        symbols = [stock["Symbol"] for stock in stocks if "Symbol" in stock]
        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/get-stock-symbols-limited")
async def get_stock_symbols_limited():
    try:
        stocks = stock_collection.find({}, {"_id": 0, "Symbol": 1}).skip(20).limit(180)
        symbols = [stock["Symbol"] for stock in stocks if "Symbol" in stock]
        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}
