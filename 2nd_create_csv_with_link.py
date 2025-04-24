from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time


def collect_yahoo_finance_news(ticker="AAPL"):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    articles = []

    base_url = f"https://finance.yahoo.com/quote/{ticker}/news?p={ticker}"
    driver.get(base_url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    news_blocks = soup.select('section[data-testid="storyitem"]')

    for block in news_blocks:
        try:
            title_tag = block.find('h3')
            link_tag = block.find('a')
            if title_tag and link_tag:
                link = link_tag['href']
                if not link.startswith('http'):
                    link = 'https://finance.yahoo.com' + link
                articles.append({'url': link})
        except Exception as e:
            print("❗ 기사 파싱 실패:", e)
            continue

    driver.quit()
    return articles


def save_news_links_to_csv(articles, filename="news_AAPL.csv"):
    df = pd.DataFrame(articles)
    df.to_csv(filename, index=False)
    print(f"✅ {len(df)}개의 링크가 {filename}에 저장되었습니다.")


if __name__ == "__main__":
    ticker = "AAPL"
    news = collect_yahoo_finance_news(ticker)

    print(f"\n✅ 총 수집된 뉴스: {len(news)}건\n")
    for i, article in enumerate(news, 1):
        print(f"[{i}] URL: {article['url']}")

    save_news_links_to_csv(news)