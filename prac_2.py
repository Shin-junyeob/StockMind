from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import prac_1

options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

url = f'https://finance.yahoo.com/quote/{prac_1.ticker}/news'
driver.get(url)
time.sleep(3)

soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

articles = soup.select('section[data-testid="storyitem"]')
print(f'\n뉴스 개수: {len(articles)}개\n')

for article in articles:
    title_tag = article.select_one('h3')
    link_tag = article.select_one('a')

    if title_tag and link_tag:
        title = title_tag.get_text(strip=True)
        link = link_tag['href']
        if not link.startswith('http'):
            link = 'https://finance.yahoo.com' + link
        print(f'제목: {title}')
        print(f'링크: {link}')