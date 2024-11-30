import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
COLLECTION_NAME = "stock_data"
STOCK_COLLECTION_NAME = "stocks"
TABLE_COLLECTION_NAME = "shareholder_data"

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

@app.post("/scrape-stocks-details")
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
    
@app.post("/scrape-shareholder-data")
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


@app.post("/scrape-annual-report")
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