from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
from typing import List

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
UL_COLLECTION_NAME = "annual_data"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
ul_collection = db[UL_COLLECTION_NAME]

# FastAPI app
app = FastAPI()

def scrape_ul_data(stock_symbol: str):
    """
    Scrapes data from the <ul> element for a single stock symbol.
    """
    URL = f"https://www.screener.in/company/{stock_symbol}/"
    response = requests.get(URL)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Locate the <ul> element with the class 'list-links'
        ul = soup.find("ul", class_="list-links")
        if not ul:
            raise Exception(f"No <ul> found with class 'list-links' for stock {stock_symbol}")
        
        # Parse <li> elements within the <ul>
        ul_data = []
        for li in ul.find_all("li"):
            link = li.find("a", href=True)
            year_text = link.text.strip() if link else "Unknown Year"
            source_text = li.find("div", class_="ink-600 smaller").text.strip() if li.find("div", class_="ink-600 smaller") else "Unknown Source"
            href = link['href'] if link else "Unknown Link"
            ul_data.append({
                "financial_year": year_text,
                "source": source_text,
                "link": href
            })
        
        # Return parsed data
        return {"stock_symbol": stock_symbol, "ul_data": ul_data}
    else:
        raise Exception(f"Failed to fetch page for {stock_symbol}. Status code: {response.status_code}")

@app.post("/scrape-multiple-ul")
async def scrape_multiple_ul_endpoint(stock_symbols: List[str]):
    """
    Scrapes the <ul> data for multiple stock symbols and saves them in MongoDB.
    """
    results = []
    for stock_symbol in stock_symbols:
        try:
            data = scrape_ul_data(stock_symbol)
            ul_collection.insert_one(data)  # Save to MongoDB
            results.append({"stock_symbol": stock_symbol, "status": "success"})
        except Exception as e:
            results.append({"stock_symbol": stock_symbol, "status": "failed", "error": str(e)})
    return {"results": results}

@app.get("/get-ul-data")
async def get_ul_data():
    """
    Fetches all <ul> data stored in the MongoDB collection.
    """
    try:
        data = list(ul_collection.find({}, {"_id": 0}))  # Exclude MongoDB ObjectId field
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
