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
SHAREHOLDER_COLLECTION_NAME = "shareholder_data"
STOCK_DETAILS_COLLECTION = "stock_deatils_data"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
stock_collection = db[STOCK_COLLECTION_NAME]
shareholder_collection = db[SHAREHOLDER_COLLECTION_NAME]
stock_details_collection = db[STOCK_DETAILS_COLLECTION]

# FastAPI app
app = FastAPI()


# Pydantic Models
class StockList(BaseModel):
    stock_symbols: str  # Comma-separated stock symbols


# Helper Functions
def fetch_page(url: str) -> BeautifulSoup:
    """
    Fetch and parse a webpage.
    """
    response = requests.get(url)
    if response.status_code == 200:
        return BeautifulSoup(response.text, "html.parser")
    raise HTTPException(status_code=404, detail=f"Failed to fetch page: {url}")


def parse_ul_top_ratios(stock_symbol: str, soup: BeautifulSoup):
    """
    Parse the <ul> with id="top-ratios" from the page.
    """
    ul_element = soup.find("ul", id="top-ratios")
    if not ul_element:
        return None

    items_dict = {}
    for li in ul_element.find_all("li"):
        name_span = li.find("span", class_="name")
        value_span = li.find("span", class_="value")
        if name_span and value_span:
            key = name_span.get_text(strip=True)
            value = value_span.get_text(strip=True)
            items_dict[key] = value

    return {"stock_symbol": stock_symbol, "stock_details": items_dict}


def parse_shareholder_table(stock_symbol: str, soup: BeautifulSoup):
    """
    Parse the shareholder table with id="quarterly-shp".
    """
    table = soup.find("div", id="quarterly-shp")
    if not table:
        return None

    headers = [th.text.strip() for th in table.find("thead").find("tr").find_all("th")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        row_data = {
            headers[idx]: cell.get_text(strip=True)
            for idx, cell in enumerate(tr.find_all("td"))
        }
        rows.append(row_data)

    return {"shareholder_data": rows}


@app.post("/scrape-all-data")
async def scrape_shareholder_data(payload: StockList):

    stock_symbols = [
        symbol.strip().upper() for symbol in payload.stock_symbols.split(",")
    ]

    async def scrape_and_save(stock_symbol):
        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            data = parse_shareholder_table(stock_symbol, soup)
            details_data = parse_ul_top_ratios(stock_symbol, soup)
            if data:
                combined_data = {
                    **data,
                    **details_data,
                }
                stock_details_collection.insert_one(combined_data)
                return {
                    "stock_symbol": stock_symbol,
                    "message": "Data scraped and saved successfully.",
                }
            return {
                "stock_symbol": stock_symbol,
                "message": "No shareholder table found.",
            }
        except Exception as e:
            return {"stock_symbol": stock_symbol, "error": str(e)}

    results = []
    for symbol in stock_symbols:
        result = await scrape_and_save(symbol)
        results.append(result)
        await asyncio.sleep(5)  # Wait for 5 seconds before processing the next stock symbol

    return {"results": results}



@app.get("/get-stock-symbols")
async def get_stock_symbols():
    """
    Fetches all stock symbols from the MongoDB 'stocks' collection
    and returns them as a comma-separated string.
    """
    try:
        # Fetch all documents from the collection
        stocks = stock_collection.find(
            {}, {"_id": 0, "Symbol": 1}
        )  # Only fetch 'Symbol' field

        # Extract symbols
        symbols = [stock["Symbol"] for stock in stocks if "Symbol" in stock]

        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")

        # Return as comma-separated string
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/get-stock-symbols-limited")
async def get_stock_symbols():
    """
    Fetches stock symbols from the MongoDB 'stocks' collection,
    returning records from 20 to 200 and returns them as a comma-separated string.
    """
    try:
        # Fetch documents starting from the 20th to the 200th (using skip and limit)
        stocks = (
            stock_collection.find({}, {"_id": 0, "Symbol": 1}).skip(2000).limit(500)
        )  # Skip 20 and limit to 180 records

        # Extract symbols
        symbols = [stock["Symbol"] for stock in stocks if "Symbol" in stock]

        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")

        # Return as comma-separated string
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}
