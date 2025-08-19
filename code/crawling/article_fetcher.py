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
    # 1) 메타 우선
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()

    meta_title = soup.find("meta", attrs={"name": "title"})
    if meta_title and meta_title.get("content"):
        return meta_title["content"].strip()

    # 2) 구조화된 h1 후보들
    for sel in (
        "h1",
        "header h1",
        "article h1",
        "div.caas-title-wrapper h1",
        "div.caas-content-header h1",
    ):
        h = soup.select_one(sel)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

    # 3) 최후: <title>
    if soup.title and soup.title.get_text():
        return soup.title.get_text(strip=True)
    return ""


def _extract_content_safely(soup: BeautifulSoup) -> str:
    # 1) 주력 본문 선택자(다중 OR)
    primary = soup.select("article p, main p, div[data-test-locator='mega'] p")
    if primary:
        return " ".join(p.get_text(strip=True) for p in primary if p.get_text(strip=True))

    # 2) fallback 후보들(구 Yahoo/caas/일반 기사 템플릿)
    fallbacks = [
        "div.caas-body p",
        "div#article-body p",
        "div[itemprop='articleBody'] p",
        "div[itemprop='articleBody'] div p",  # 중첩 구조
        "section[data-test-locator='mega'] p",
    ]
    for sel in fallbacks:
        nodes = soup.select(sel)
        if nodes:
            return " ".join(p.get_text(strip=True) for p in nodes if p.get_text(strip=True))

    # 3) 최후의 수단: meta description
    meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    # 4) 정말 없으면 빈 문자열(상위에서 "본문 없음" 처리)
    return ""


def _parse_datetime_kst(soup: BeautifulSoup) -> str:
    # 1) 가장 신뢰할 수 있는 time[datetime]
    t = soup.select_one("time[datetime]")
    if t and t.has_attr("datetime"):
        try:
            utc_iso = t["datetime"].replace("Z", "+00:00")
            kst = pd.to_datetime(utc_iso).tz_convert("Asia/Seoul").to_pydatetime()
            return kst.strftime("%Y-%m-%d")
        except Exception:
            pass

    # 2) 기사 메타에 자주 쓰이는 퍼블리시드 타임
    for meta_name in (
        {"property": "article:published_time"},
        {"name": "article:published_time"},
        {"name": "publish-date"},
        {"itemprop": "datePublished"},
    ):
        meta = soup.find("meta", attrs=meta_name)
        if meta and meta.get("content"):
            try:
                utc_iso = meta["content"].replace("Z", "+00:00")
                kst = pd.to_datetime(utc_iso).tz_convert("Asia/Seoul").to_pydatetime()
                return kst.strftime("%Y-%m-%d")
            except Exception:
                continue

    # 3) 실패 시 오늘 날짜
    return dt.datetime.now().strftime("%Y-%m-%d")

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
