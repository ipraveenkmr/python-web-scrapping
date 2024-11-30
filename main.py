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
UL_COLLECTION_NAME = "annual_data"
TABLE_COLLECTION_NAME = "shareholder_data"
TOP_RATIOS_COLLECTION_NAME = "top_ratios"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
stock_collection = db[STOCK_COLLECTION_NAME]
ul_collection = db[UL_COLLECTION_NAME]
table_collection = db[TABLE_COLLECTION_NAME]
ratios_collection = db[TOP_RATIOS_COLLECTION_NAME]

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

    return {"stock_symbol": stock_symbol, "items": items_dict}


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
        row_data = {headers[idx]: cell.get_text(strip=True) for idx, cell in enumerate(tr.find_all("td"))}
        rows.append(row_data)

    return {"stock_symbol": stock_symbol, "table_data": rows}


def parse_annual_data(stock_symbol: str, soup: BeautifulSoup):
    """
    Parse the <ul> with class="list-links".
    """
    ul = soup.find("ul", class_="list-links")
    if not ul:
        return None

    ul_data = []
    for li in ul.find_all("li"):
        link = li.find("a", href=True)
        year_text = link.text.strip() if link else "Unknown Year"
        source_text = li.find("div", class_="ink-600 smaller").text.strip() if li.find("div", class_="ink-600 smaller") else "Unknown Source"
        href = link['href'] if link else "Unknown Link"
        ul_data.append({"financial_year": year_text, "source": source_text, "link": href})

    return {"stock_symbol": stock_symbol, "ul_data": ul_data}


# API Endpoints
@app.post("/scrape-stocks-details")
async def scrape_stocks_details(payload: StockList):
    """
    Scrapes top ratios data for a list of stock symbols.
    """
    stock_symbols = [symbol.strip().upper() for symbol in payload.stock_symbols.split(",")]
    results = []

    for stock_symbol in stock_symbols:
        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            data = parse_ul_top_ratios(stock_symbol, soup)
            if data:
                ratios_collection.insert_one(data)
                results.append({"stock_symbol": stock_symbol, "message": "Data scraped and saved successfully."})
            else:
                results.append({"stock_symbol": stock_symbol, "message": "No top ratios found."})
        except Exception as e:
            results.append({"stock_symbol": stock_symbol, "error": str(e)})

    return {"results": results}


@app.post("/scrape-shareholder-data")
async def scrape_shareholder_data(payload: StockList):
    """
    Scrapes shareholder data for a list of stock symbols.
    """
    stock_symbols = [symbol.strip().upper() for symbol in payload.stock_symbols.split(",")]

    async def scrape_and_save(stock_symbol):
        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            data = parse_shareholder_table(stock_symbol, soup)
            if data:
                table_collection.insert_one(data)
                return {"stock_symbol": stock_symbol, "message": "Data scraped and saved successfully."}
            return {"stock_symbol": stock_symbol, "message": "No shareholder table found."}
        except Exception as e:
            return {"stock_symbol": stock_symbol, "error": str(e)}

    tasks = [scrape_and_save(symbol) for symbol in stock_symbols]
    results = await asyncio.gather(*tasks)

    return {"results": results}


@app.post("/scrape-annual-report")
async def scrape_annual_report(payload: StockList):
    """
    Scrapes annual report data for a list of stock symbols.
    """
    stock_symbols = [symbol.strip().upper() for symbol in payload.stock_symbols.split(",")]
    results = []

    for stock_symbol in stock_symbols:
        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            data = parse_annual_data(stock_symbol, soup)
            if data:
                ul_collection.insert_one(data)
                results.append({"stock_symbol": stock_symbol, "message": "Annual data scraped and saved."})
            else:
                results.append({"stock_symbol": stock_symbol, "message": "No annual data found."})
        except Exception as e:
            results.append({"stock_symbol": stock_symbol, "error": str(e)})

    return {"results": results}


@app.get("/get-saved-data")
async def get_saved_data():
    """
    Fetches all saved data from MongoDB.
    """
    try:
        data = list(ratios_collection.find({}, {"_id": 0}))
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    

@app.get("/get-stock-symbols")
async def get_stock_symbols():
    """
    Fetches all stock symbols from the MongoDB 'stocks' collection 
    and returns them as a comma-separated string.
    """
    try:
        # Fetch all documents from the collection
        stocks = stock_collection.find({}, {"_id": 0, "Symbol": 1})  # Only fetch 'Symbol' field
        
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
        stocks = stock_collection.find({}, {"_id": 0, "Symbol": 1}).skip(2000).limit(500)  # Skip 20 and limit to 180 records
        
        # Extract symbols
        symbols = [stock["Symbol"] for stock in stocks if "Symbol" in stock]
        
        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")
        
        # Return as comma-separated string
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}

    
