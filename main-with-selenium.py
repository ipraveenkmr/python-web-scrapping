import asyncio
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "scraping_db"
STOCK_COLLECTION_NAME = "stocks"
STOCK_DETAILS_COLLECTION = "stock_details_21_03"
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
    

def ratios_table(stock_symbol: str, soup: BeautifulSoup):
    try:
        table = soup.find("section", id="ratios")
        if not table:
            print(f"No ratios section found for {stock_symbol}.")
            return {"ratios_result": []}

        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"ratios_result": []}

        # Extract headers
        headers = []
        header_row = data_table.find("thead").find("tr")
        if not header_row:
            print(f"No header row found for {stock_symbol}.")
            return {"ratios_result": []}

        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True))

        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"ratios_result": []}

        # Extract rows
        rows = []
        tbody = data_table.find("tbody")
        if not tbody:
            print(f"No tbody found for {stock_symbol}.")
            return {"ratios_result": []}

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) == 0:
                continue

            row_label = cells[0].get_text(strip=True)  # First cell is the ratio name
            row_data = {headers[idx]: cell.get_text(strip=True) for idx, cell in enumerate(cells)}
            row_data["Ratio Name"] = row_label  # Add the ratio name separately
            rows.append(row_data)

        if not rows:
            print(f"No data rows found for {stock_symbol}.")
            return {"ratios_result": []}

        return {"ratios_result": rows}
    except Exception as e:
        print(f"Error parsing ratios table for {stock_symbol}: {str(e)}")
        return {"ratios_result": []}

def parse_peer_comparison_table(stock_symbol: str, html_content: str):
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        table = soup.find("section", id="peers")
        
        if not table:
            print(f"No peers section found for {stock_symbol}.")
            return {"peers": []}

        data_table = table.find("table", class_="data-table")
        if not data_table:
            print(f"No data table found for {stock_symbol}.")
            return {"peers": []}

        headers = [th.get_text(strip=True) for th in data_table.find_all("th")]
        if not headers:
            print(f"No headers found for {stock_symbol}.")
            return {"peers": []}

        rows = []
        for tr in data_table.find("tbody").find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) != len(headers):
                print(f"Row mismatch for {stock_symbol}: {cells}")
                continue
            row_data = {headers[idx]: cell.get_text(strip=True) for idx, cell in enumerate(cells)}
            rows.append(row_data)

        return {"peers": rows}
    except Exception as e:
        print(f"Error parsing peer comparison table for {stock_symbol}: {str(e)}")
        return {"peers": []}
    
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


def scrape_credit_ratings(url):
    """Scrape credit ratings from the given URL."""
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch {url} - Status Code: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        reports_section = soup.find("div", class_="documents credit-ratings flex-column")
        if not reports_section:
            print("[ERROR] No credit ratings section found.")
            return []

        report_links = []
        for li in reports_section.find_all("li"):
            a_tag = li.find("a")
            date_tag = li.find("div", class_="ink-600 smaller")

            if a_tag and a_tag.get("href") and date_tag:
                report_links.append({
                    "date": date_tag.text.strip(),
                    "url": a_tag["href"]
                })

        return report_links

    except Exception as e:
        print(f"[EXCEPTION] {str(e)}")
        return []
    


def scrape_concalls(url):
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve the page")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    concalls = []
    
    concall_section = soup.find('div', class_='documents concalls flex-column')
    if not concall_section:
        print("No concall section found")
        return []
    
    for item in concall_section.find_all('li', class_='flex'):
        date = item.find('div', class_='ink-600').text.strip()
        
        transcript = item.find('a', class_='concall-link', title="Raw Transcript")
        transcript_url = transcript['href'] if transcript else None
        
        notes_button = item.find('button', class_='concall-link')
        notes_url = notes_button['data-url'] if notes_button else None
        
        ppt = None
        for link in item.find_all('a', class_='concall-link'):
            if 'PPT' in link.text:
                ppt = link['href']
                break
        
        rec = None
        for link in item.find_all('a', class_='concall-link'):
            if 'mp3' in link['href']:
                rec = link['href']
                break
        
        concalls.append({
            'date': date,
            'transcript': transcript_url,
            'notes': notes_url,
            'ppt': ppt,
            'rec': rec
        })
    
    return concalls

@app.post("/scrape-all-datas")
async def scrape_shareholder_data(payload: StockList):

    stock_symbols = [
        symbol.strip().upper() for symbol in payload.stock_symbols.split(",")
    ]

    def scrape_and_save(stock_symbol, driver):
        peer_data = []

        try:
            url = f"https://www.screener.in/company/{stock_symbol}/consolidated/"
            driver.get(url)
            driver.refresh()
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "peers"))
            )
            
            html_content = driver.page_source
            peer_data = parse_peer_comparison_table(stock_symbol, html_content)
            
            # stock_details_collection.insert_one(peer_data)

        except Exception as e:
            print(f"Error fetching peer data for {stock_symbol}: {str(e)}")
        finally:
            driver.quit()

        try:
            soup = fetch_page(f"https://www.screener.in/company/{stock_symbol}/")
            details_data = parse_ul_top_ratios(stock_symbol, soup)
            shareholder_data = parse_shareholder_table(stock_symbol, soup)
            profit_loss_data = parse_profit_loss_table(stock_symbol, soup)
            balance_sheet_data = parse_balance_sheet_table(stock_symbol, soup)
            quaterly_result_data = parse_quaterly_result_table(stock_symbol, soup)
            shareholding_data = shareholding_table(stock_symbol, soup)
            cashflow_data = cashflow_table(stock_symbol, soup)
            ratios_data = ratios_table(stock_symbol, soup)

            annual_reports_url = f"https://www.screener.in/company/{stock_symbol}/consolidated/"
            annual_reports_data = scrape_annual_reports(annual_reports_url)
            credit_ratings_data = scrape_credit_ratings(annual_reports_url)
            scrape_concalls_data = scrape_concalls(annual_reports_url)

            if details_data or shareholder_data:
                combined_data = {
                    **details_data,
                    **shareholder_data,
                    **profit_loss_data,
                    **balance_sheet_data,
                    **quaterly_result_data,
                    **shareholding_data,
                    **cashflow_data,
                    **ratios_data,
                    **peer_data,
                    "annual_reports": annual_reports_data,
                    "credit_ratings": credit_ratings_data,
                    "scrape_concalls": scrape_concalls_data,
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

    # ✅ Process all stocks concurrently
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    try:
        results = await asyncio.gather(*(scrape_and_save(symbol, driver) for symbol in stock_symbols))
    finally:
        driver.quit()

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
