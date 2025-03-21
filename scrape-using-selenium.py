from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up WebDriver (Make sure you have ChromeDriver installed)
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode
driver = webdriver.Chrome(options=options)

try:
    url = "https://www.screener.in/company/TCS/consolidated/"
    driver.get(url)

    # Refresh the page
    driver.refresh()

    # Wait for the "peers" section to appear
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "peers"))
    )

    # Extract Peer Comparison section
    peers_section = driver.find_element(By.ID, "peers").text
    print(peers_section)

finally:
    driver.quit()
