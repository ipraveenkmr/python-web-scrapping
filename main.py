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
STOCK_DETAILS_COLLECTION = "stock_details_19_03"
# STOCK_DETAILS_COLLECTION = "updated_stock_details"
EQUITY_LIST = "equity_list_nse"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
stock_collection = db[STOCK_COLLECTION_NAME]
stock_details_collection = db[STOCK_DETAILS_COLLECTION]
equity_list = db[EQUITY_LIST]

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


def parse_profit_loss_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="profit-loss")
        # table = soup.find("div", id="peers-table-placeholder")
        if not table:
            print(f"No table placeholder found for {stock_symbol}.")
            return {"profit_loss": []}

        # Find the table within the placeholder
        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"profit_loss": []}

        # Extract headers
        headers = []
        header_row = data_table.find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"profit_loss": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"profit_loss": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"profit_loss": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue

            # Map headers to cell values
            row_data = {
                headers[idx]: cell.get_text(strip=True)
                for idx, cell in enumerate(cells)
            }
            rows.append(row_data)

        if not rows:
            print(f"No data rows found for {stock_symbol}.")
            return {"profit_loss": []}

        return {"profit_loss": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"profit_loss": []}


def parse_balance_sheet_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="balance-sheet")
        # table = soup.find("div", id="peers-table-placeholder")
        if not table:
            print(f"No table placeholder found for {stock_symbol}.")
            return {"balance_sheet": []}

        # Find the table within the placeholder
        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"balance_sheet": []}

        # Extract headers
        headers = []
        header_row = data_table.find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"balance_sheet": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"balance_sheet": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"balance_sheet": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue

            # Map headers to cell values
            row_data = {
                headers[idx]: cell.get_text(strip=True)
                for idx, cell in enumerate(cells)
            }
            rows.append(row_data)

        if not rows:
            print(f"No data rows found for {stock_symbol}.")
            return {"balance_sheet": []}

        return {"balance_sheet": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"balance_sheet": []}


def parse_quaterly_result_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="quarters")
        # table = soup.find("div", id="peers-table-placeholder")
        if not table:
            print(f"No table placeholder found for {stock_symbol}.")
            return {"quarterly_result": []}

        # Find the table within the placeholder
        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"quarterly_result": []}

        # Extract headers
        headers = []
        header_row = data_table.find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"quarterly_result": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"quarterly_result": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"quarterly_result": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue

            # Map headers to cell values
            row_data = {
                headers[idx]: cell.get_text(strip=True)
                for idx, cell in enumerate(cells)
            }
            rows.append(row_data)

        if not rows:
            print(f"No data rows found for {stock_symbol}.")
            return {"quarterly_result": []}

        return {"quarterly_result": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"quarterly_result": []}
    
    
def shareholding_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="shareholding")
        # table = soup.find("div", id="peers-table-placeholder")
        if not table:
            print(f"No table placeholder found for {stock_symbol}.")
            return {"shareholding_result": []}

        # Find the table within the placeholder
        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"shareholding_result": []}

        # Extract headers
        headers = []
        header_row = data_table.find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"shareholding_result": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"shareholding_result": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"shareholding_result": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue

            # Map headers to cell values
            row_data = {
                headers[idx]: cell.get_text(strip=True)
                for idx, cell in enumerate(cells)
            }
            rows.append(row_data)

        if not rows:
            print(f"shareholding_result No data rows found for {stock_symbol}.")
            return {"shareholding_result": []}

        return {"shareholding_result": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"shareholding_result": []}
    
def cashflow_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="cash-flow")
        # table = soup.find("div", id="peers-table-placeholder")
        if not table:
            print(f"No table placeholder found for {stock_symbol}.")
            return {"cashflow_result": []}

        # Find the table within the placeholder
        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"cashflow_result": []}

        # Extract headers
        headers = []
        header_row = data_table.find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"cashflow_result": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"cashflow_result": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"cashflow_result": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue

            # Map headers to cell values
            row_data = {
                headers[idx]: cell.get_text(strip=True)
                for idx, cell in enumerate(cells)
            }
            rows.append(row_data)

        if not rows:
            print(f"cashflow_result No data rows found for {stock_symbol}.")
            return {"cashflow_result": []}

        return {"cashflow_result": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"cashflow_result": []}


def parse_peer_comparision_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table_container = soup.find("div", id="peers-table-placeholder")
        if not table_container:
            print(f"[ERROR] No table placeholder found for {stock_symbol}.")
            return {"peer_comparision": []}

        # Locate the actual table inside the placeholder
        data_table = table_container.find("table", class_="data-table")
        if not data_table:
            print(f"[ERROR] No data table found for {stock_symbol}.")
            return {"peer_comparision": []}

        # Extract headers (Look for the first row with <th> elements)
        header_row = data_table.find(
            "tr"
        )  # Finds first <tr> (no <thead> in provided HTML)
        if not header_row:
            print(f"[ERROR] No header row found for {stock_symbol}.")
            return {"peer_comparision": []}

        headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
        if not headers:
            print(f"[ERROR] No headers found for {stock_symbol}.")
            return {"peer_comparision": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"[ERROR] No tbody found for {stock_symbol}.")
            return {"peer_comparision": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")

            # Ensure we only extract rows with valid data
            if len(cells) == 0:
                continue  # Skip empty rows

            row_data = {
                headers[i]: (cells[i].get_text(strip=True) if i < len(cells) else "N/A")
                for i in range(len(headers))
            }

            rows.append(row_data)

        if not rows:
            print(
                f"[WARNING] No data rows found in Peer Comparison for {stock_symbol}."
            )
            return {"peer_comparision": []}

        return {"peer_comparision": rows}

    except Exception as e:
        print(
            f"[EXCEPTION] Error parsing peer comparison table for {stock_symbol}: {str(e)}"
        )
        return {"peer_comparision": []}


def scrape_annual_reports(url):
    """Scrape annual reports from the given URL."""
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch {url} - Status Code: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        reports_section = soup.find("div", class_="documents annual-reports flex-column")
        if not reports_section:
            print("[ERROR] No annual reports section found.")
            return []

        report_links = []
        for li in reports_section.find_all("li"):
            a_tag = li.find("a")
            if a_tag and a_tag.get("href"):
                year_text = a_tag.text.strip()
                year = next((word for word in year_text.split() if word.isdigit()), "Unknown Year")
                link = a_tag["href"]

                # Ensure absolute URL
                if link.startswith("/"):
                    link = f"https://www.bseindia.com{link}"

                report_links.append({"year": year, "url": link})

        return report_links

    except Exception as e:
        print(f"[EXCEPTION] {str(e)}")
        return []


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
            profit_loss_data = parse_profit_loss_table(stock_symbol, soup)
            balance_sheet_data = parse_balance_sheet_table(stock_symbol, soup)
            quaterly_result_data = parse_quaterly_result_table(stock_symbol, soup)
            peer_comparision_data = parse_peer_comparision_table(stock_symbol, soup)
            shareholding_data = shareholding_table(stock_symbol, soup)
            cashflow_data = cashflow_table(stock_symbol, soup)
            
            annual_reports_url = f"https://www.screener.in/company/{stock_symbol}/consolidated/"
            annual_reports_data = scrape_annual_reports(annual_reports_url)
            
            if details_data or shareholder_data:
                combined_data = {
                    **details_data,
                    **shareholder_data,
                    **profit_loss_data,
                    **balance_sheet_data,
                    **quaterly_result_data,
                    **peer_comparision_data,
                    **shareholding_data,
                    **cashflow_data,
                    "annual_reports": annual_reports_data,
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
        await asyncio.sleep(
            5
        )  # Wait for 5 seconds before processing the next stock symbol

    return {"results": results}


@app.get("/get-stock-symbols")
async def get_stock_symbols():
    """
    Fetches all stock symbols from the MongoDB 'stocks' collection
    and returns them as a comma-separated string.
    """
    try:
        # Fetch all documents from the collection
        stocks = equity_list.find(
            {}, {"_id": 0, "SYMBOL": 1}
        )  # Only fetch 'Symbol' field

        # Extract symbols
        symbols = [stock["SYMBOL"] for stock in stocks if "SYMBOL" in stock]

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
            equity_list.find({}, {"_id": 0, "SYMBOL": 1}).skip(2000).limit(500)
        )  # Skip 20 and limit to 180 records

        # Extract symbols
        symbols = [stock["SYMBOL"] for stock in stocks if "SYMBOL" in stock]

        if not symbols:
            raise HTTPException(status_code=404, detail="No stock symbols found.")

        # Return as comma-separated string
        return {"symbols": ", ".join(symbols)}
    except Exception as e:
        return {"error": str(e)}
