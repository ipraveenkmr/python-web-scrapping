from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from bs4 import BeautifulSoup
import asyncio
import requests

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
TABLE_COLLECTION_NAME = "table_data"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
table_collection = db[TABLE_COLLECTION_NAME]

# FastAPI app
app = FastAPI()

# Pydantic Model for Request Payload
class StockList(BaseModel):
    stock_symbols: str

def scrape_table_data(stock_symbol: str):
    """
    Scrapes table data from a given stock symbol's page and returns it in JSON format.
    """
    URL = f"https://www.screener.in/company/{stock_symbol}/"
    response = requests.get(URL)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Locate the table
        table = soup.find("div", id="quarterly-shp")
        if not table:
            raise Exception("No table found with id 'quarterly-shp'")
        
        # Parse table headers
        headers = []
        header_row = table.find("thead").find("tr")
        for th in header_row.find_all("th"):
            headers.append(th.text.strip())
        
        # Parse table rows
        rows = []
        for tr in table.find("tbody").find_all("tr"):
            row_data = {}
            cells = tr.find_all(["td", "th"])
            for idx, cell in enumerate(cells):
                if idx == 0:  # First cell is the row identifier
                    row_data["Category"] = cell.get_text(strip=True)
                else:
                    row_data[headers[idx]] = cell.get_text(strip=True)
            rows.append(row_data)
        
        # Return parsed data
        return {"stock_symbol": stock_symbol, "table_data": rows}
    else:
        raise Exception(f"Failed to fetch page for {stock_symbol}. Status code: {response.status_code}")

@app.post("/scrape-table")
async def scrape_table(stock_symbol: str):
    """
    Scrapes the table data for a given stock symbol and saves it in MongoDB.
    """
    try:
        data = scrape_table_data(stock_symbol)
        # Save data to MongoDB
        table_collection.insert_one(data)
        return {"message": f"Table data for {stock_symbol} scraped and saved successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-table-data")
async def get_table_data():
    """
    Fetches all table data stored in the MongoDB collection.
    """
    try:
        data = list(table_collection.find({}, {"_id": 0}))  # Exclude MongoDB ObjectId field
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape-multiple-stocks")
async def scrape_multiple_stocks(payload: StockList):
    """
    Scrapes data for a list of stock symbols provided as a comma-separated string
    and saves it to MongoDB asynchronously.
    """
    stock_symbols = [symbol.strip().upper() for symbol in payload.stock_symbols.split(",")]
    if not stock_symbols:
        raise HTTPException(status_code=400, detail="No stock symbols provided.")

    async def scrape_and_save(stock_symbol):
        """
        Helper function to scrape data for a single stock and save to MongoDB.
        """
        try:
            data = scrape_table_data(stock_symbol)
            if data:
                # Save to MongoDB
                table_collection.insert_one(data)
                return {"stock_symbol": stock_symbol, "message": "Data scraped and saved successfully."}
            else:
                return {"stock_symbol": stock_symbol, "message": "No table found with id 'quarterly-shp'."}
        except Exception as e:
            return {"stock_symbol": stock_symbol, "error": str(e)}

    # Execute scraping tasks concurrently for all stock symbols
    tasks = [scrape_and_save(symbol) for symbol in stock_symbols]
    results = await asyncio.gather(*tasks)

    return {"results": results}
