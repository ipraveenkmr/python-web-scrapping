from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

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

# Set up Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

try:
    stock_symbol = "TCS"
    url = f"https://www.screener.in/company/{stock_symbol}/consolidated/"
    driver.get(url)
    driver.refresh()
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "peers"))
    )
    
    html_content = driver.page_source
    peer_data = parse_peer_comparison_table(stock_symbol, html_content)
    print(peer_data)

finally:
    driver.quit()
