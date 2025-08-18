import time, random, datetime as dt
from typing import Iterable, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from .http_utils import make_session, http_get, UARotator
from .settings import UA_LIST, SELENIUM, ACCEPT_LANGUAGE


# 간단한 셀레니움 폴백(본문이 너무 짧을 때만)
def _build_headless_driver(user_agent: Optional[str] = None) -> webdriver.Chrome:
    opts = Options()
    if SELENIUM.get("headless", True):
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    w, h = SELENIUM.get("window_size", (1920, 1080))
    opts.add_argument(f"--window-size={w}x{h}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    return webdriver.Chrome(options=opts)

def _extract_title_safely(soup: BeautifulSoup) -> str:
    # 1순위: OG title
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()

    # 2순위: meta name=title
    meta_title = soup.find("meta", attrs={"name": "title"})
    if meta_title and meta_title.get("content"):
        return meta_title["content"].strip()

    # 3순위: h1 / 기사 타이틀로 자주 쓰이는 선택자들 (필요시 보강(
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)

    # 4순위: <title>
    if soup.title and soup.title.get_text():
        return soup.title.get_text(strip=True)

    return ""

def _parse_datetime_kst(soup: BeautifulSoup) -> str:
    t = soup.select_one("time.byline-attr-meta-time,[datetime]")
    if t and t.has_attr("datetime"):
        try:
            # ex) 2025-08-15T12:34:56Z
            utc_iso = t["datetime"].replace("Z", "+00:00")
            kst = pd.to_datetime(utc_iso).tz_convert("Asia/Seoul").to_pydatetime()
            return kst.strftime("%Y-%m-%d")
        except Exception:
            pass
    if meta_time and meta_time.get("content"):
        try:
            utc_iso = meta_time["content"].replace("Z", "+00:00")
            kst = pd.to_datetime(utc_iso).tz_convert("Asia/Seoul").to_pydatetime()
            return kst.strftime("%Y-%m-%d")
        except Exception:
            pass
    return dt.datetime.now().strftime("%Y-%m-%d")

def _extract_content_safely(soup: BeautifulSoup) -> str:
    # 1순위: 본문 p
    paras = [p.get_text(strip=True) for p in soup.select("article p, main p, div[data-test-locator='mega'] p")]
    if paras:
        return " ".join(paras)

    # 2순위: meta description
    meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
    if meta and meta.get("content"):
        return meta["content"]

    # 3순위: title만이라도
    if soup.title:
        return soup.title.get_text(strip=True)

    return ""

def fetch_articles_http(urls: Iterable[str], ua_mode: str = "round_robin", delay_range=(0.8, 1.6), min_len_for_ok: int = 120, enable_selenium_fallback: bool = True) -> list[dict]:
    rotator = UARotator(UA_LIST, ua_mode)
    session = make_session()
    results = []
    for u in urls:
        try:
            resp = http_get(u, session=session, ua_rotator=rotator)
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            content = _extract_content_safely(soup)
            date_str = _parse_datetime_kst(soup)
            title = _extract_title_safely(soup)
            status = resp.status_code

            # 요청 결과가 빈약하면(짧거나 paywall 등) 선택적으로 Selenium 폴백
            if enable_selenium_fallback and len(content) < min_len_for_ok and "finance.yahoo.com" in u:
                driver = _build_headless_driver(user_agent=rotator.pick())
                try:
                    driver.set_page_load_timeout(SELENIUM.get("page_load_timeout", 180))
                    driver.get(u)
                    time.sleep(1.5)
                    soup2 = BeautifulSoup(driver.page_source, "html.parser")
                    content2 = _extract_content_safely(soup2)
                    if len(content2) > len(content):
                        content = content2
                        date_str = _parse_datetime_kst(soup2) or date_str
                        title2 = _extract_title_safely(soup2)
                        if title2:
                            title = title2
                finally:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            results.append({
                "url": u,
                "title": title or "",
                "content": content or "본문 없음",
                "date": date_str,
                "status_code": status,
            })
        except Exception as e:
            results.append({"url": u, "error": str(e), "date": dt.datetime.now().strftime("%Y-%m-%d")})
        time.sleep(random.uniform(*delay_range))
    return results
