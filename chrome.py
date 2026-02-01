from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = Options()
chrome_options.add_argument('--headless=new')

try:
    print("🔧 Installing/locating ChromeDriver...")
    service = Service(ChromeDriverManager().install())
    
    print("🚀 Starting Chrome WebDriver...")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    print("✅ Chrome WebDriver works!")
    driver.get("https://www.google.com")
    print(f"✅ Page title: {driver.title}")
    driver.quit()
    print("✅ All tests passed!")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()