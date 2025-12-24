try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    print("Selenium imported successfully.")
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    print("Chrome Driver initialized successfully.")
    driver.quit()
except Exception as e:
    print(f"Selenium check failed: {e}")
