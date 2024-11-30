import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
COLLECTION_NAME = "ul_data"
STOCK_COLLECTION_NAME = "stocks"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]
stock_collection = db[STOCK_COLLECTION_NAME]

# FastAPI app
app = FastAPI()

# Pydantic model for request payload
class StockList(BaseModel):
    stock_symbols: str  # Comma-separated stock symbols

# Pydantic model for MongoDB data
class ULData(BaseModel):
    stock_symbol: str
    content: str
    items: dict

def scrape_ul_with_id_top_ratios(stock_symbol: str):
    """
    Scrapes the <ul> with id="top-ratios" for a given stock symbol.
    """
    URL = f"https://www.screener.in/company/{stock_symbol}/"
    response = requests.get(URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        ul_element = soup.find("ul", id="top-ratios")
        if ul_element:
            ul_content = ul_element.get_text(strip=True)

            # Parse <li> elements inside <ul>
            items_dict = {}
            li_elements = ul_element.find_all("li")
            for li in li_elements:
                # Extract spans inside each <li>
                name_span = li.find("span", class_="name")
                value_span = li.find("span", class_="value")
                
                if name_span and value_span:
                    key = name_span.get_text(strip=True)
                    value = value_span.get_text(strip=True)
                    items_dict[key] = value

            return {
                "stock_symbol": stock_symbol,
                "content": ul_content,
                "items": items_dict,
            }
        else:
            return None
    else:
        raise Exception(f"Failed to fetch the page for {stock_symbol}. Status code: {response.status_code}")

@app.post("/scrape-stocks")
async def scrape_multiple_stocks(payload: StockList):
    """
    Scrapes data for a list of stock symbols provided as a comma-separated string
    and saves it to MongoDB.
    """
    stock_symbols = [symbol.strip().upper() for symbol in payload.stock_symbols.split(",")]
    if not stock_symbols:
        raise HTTPException(status_code=400, detail="No stock symbols provided.")

    results = []
    for stock_symbol in stock_symbols:
        try:
            data = scrape_ul_with_id_top_ratios(stock_symbol)
            if data:
                # Save to MongoDB
                collection.insert_one(data)
                results.append({"stock_symbol": stock_symbol, "message": "Data scraped and saved successfully."})
            else:
                results.append({"stock_symbol": stock_symbol, "message": "No <ul> with id='top-ratios' found."})
        except Exception as e:
            results.append({"stock_symbol": stock_symbol, "error": str(e)})

    return {"results": results}

@app.get("/get-data")
async def get_saved_data():
    """
    Fetches all saved data from the MongoDB collection.
    """
    try:
        saved_data = list(collection.find({}, {"_id": 0}))  # Exclude the MongoDB ObjectId field
        return {"data": saved_data}
    except Exception as e:
        return {"error": str(e)}


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