import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time


def get_article_content(driver, url):
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        paragraphs = soup.select('p.yf-1090901')
        text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        return text if text else '본문 없음'

    except Exception as e:
        print(f"❗ 본문 수집 실패 ({url}):", e)
        return "본문 수집 실패"


def enrich_articles_with_content(file_name='news_AAPL.csv'):
    df = pd.read_csv(file_name)
    df['content'] = ""

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    for i, row in df.iterrows():
        content = get_article_content(driver, row['url'])
        df.at[i, 'content'] = content
        time.sleep(1)

    driver.quit()
    df.to_csv(file_name, index=False)
    print(f"✅ 본문이 포함된 뉴스 {len(df)}건이 {file_name}에 저장되었습니다.")


if __name__ == "__main__":
    enrich_articles_with_content()