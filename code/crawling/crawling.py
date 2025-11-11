# code/crawling/crawling.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import gc
import time
import random
import datetime as dt
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException, TimeoutException

# 프로젝트 설정/함수
from .settings import RAW_DIR, UA_LIST, STOP_LOOKBACK_DAYS, STOP_TOPN
from .article_fetcher import fetch_articles_http

# ───────────────────────────────────────────────────────────────────────────────
# (선택) 리소스 모니터: psutil이 있으면 실행 후 요약 출력
# ───────────────────────────────────────────────────────────────────────────────
try:
    import psutil
except Exception:
    psutil = None


class ResourceMonitor:
    """실행 시간/CPU/메모리(피크) 요약용. psutil 없으면 No-Op."""
    def __init__(self, label: str = "run"):
        self.label = label
        self.enabled = psutil is not None
        self._proc = psutil.Process(os.getpid()) if self.enabled else None
        self._start = None
        self._peak_rss = 0
        self._peak_swap = 0
        self._peak_ram_pct = 0.0
        self._cpu_samples = []
        self._last_cpu_time = None

    def __enter__(self):
        self._start = time.perf_counter()
        if self.enabled:
            # 첫 샘플
            mem = self._proc.memory_info().rss
            self._peak_rss = max(self._peak_rss, mem)
            vm = psutil.virtual_memory()
            self._peak_ram_pct = max(self._peak_ram_pct, vm.percent)
            sw = psutil.swap_memory()
            self._peak_swap = max(self._peak_swap, sw.used)
            self._last_cpu_time = self._proc.cpu_times()
        return self

    def sample(self):
        if not self.enabled:
            return
        try:
            mem = self._proc.memory_info().rss
            self._peak_rss = max(self._peak_rss, mem)
            vm = psutil.virtual_memory()
            self._peak_ram_pct = max(self._peak_ram_pct, vm.percent)
            sw = psutil.swap_memory()
            self._peak_swap = max(self._peak_swap, sw.used)

            # 간단한 CPU 사용률 샘플(프로세스 기준 추정)
            now_cpu = self._proc.cpu_times()
            if self._last_cpu_time:
                delta_user = now_cpu.user - self._last_cpu_time.user
                delta_sys = now_cpu.system - self._last_cpu_time.system
                self._cpu_samples.append(max(0.0, delta_user + delta_sys))
            self._last_cpu_time = now_cpu
        except Exception:
            pass

    def tick(self):
        # 필요 시 중간중간 호출(티커 종료 시 등)
        self.sample()

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.perf_counter() - self._start if self._start else 0.0
        # CPU 평균(대략적인 추정; 샘플 간 간격을 고려하지 않은 누적 시간 기반)
        cpu_total = sum(self._cpu_samples)
        cpu_avg = (cpu_total / elapsed) * 100.0 if self.enabled and elapsed > 0 else 0.0

        def _mb(x: int) -> float:
            return round(x / (1024 * 1024), 1)

        print("\n──── Resource Summary [{}] ────".format(self.label))
        print(f"⏱  Elapsed: {elapsed:.2f} sec")
        if self.enabled:
            print(f"🧠  Peak RSS: {_mb(self._peak_rss)} MB")
            print(f"💾  Peak Swap Used: {_mb(self._peak_swap)} MB")
            print(f"📈  Peak RAM Usage (system): {self._peak_ram_pct:.1f}%")
            print(f"⚙️  CPU (rough avg): {cpu_avg:.1f}%")
        else:
            print("ℹ️  psutil 미설치 → 메모리/CPU 요약 생략됨 (pip install psutil 권장)")
        print("────────────────────────────────\n")


# ───────────────────────────────────────────────────────────────────────────────
# 상수/유틸
# ───────────────────────────────────────────────────────────────────────────────
DATE_FMT = "%Y-%m-%d"
YF_BASE_NEWS_URL = "https://finance.yahoo.com/quote/{ticker}/news?p={ticker}"


def _norm_url(u: str | None) -> str:
    return str(u).strip() if u is not None else ""


def _last_date_dir(base_dir: str, ticker: str) -> Optional[str]:
    """
    data/raw/{ticker}/ 아래 YYYY-MM-DD 폴더 중 가장 최근 날짜를 반환. 없으면 None.
    """
    tdir = os.path.join(base_dir, ticker)
    if not os.path.exists(tdir):
        return None
    dates = []
    for name in os.listdir(tdir):
        p = os.path.join(tdir, name)
        if not os.path.isdir(p):
            continue
        try:
            dt.datetime.strptime(name, DATE_FMT)
            dates.append(name)
        except ValueError:
            pass
    return max(dates) if dates else None


# ───────────────────────────────────────────────────────────────────────────────
# stop set: url 컬럼만 가볍게 부분 로드
# ───────────────────────────────────────────────────────────────────────────────
def _load_urls_head(save_dir: str, top_n: int) -> list[str]:
    pq = os.path.join(save_dir, "news.parquet")
    csv = os.path.join(save_dir, "news.csv")
    try:
        if os.path.exists(pq):
            df = pd.read_parquet(pq, columns=["url"])
            return df["url"].dropna().astype(str).str.strip().head(top_n).tolist()
        if os.path.exists(csv):
            df = pd.read_csv(csv, usecols=["url"], nrows=top_n * 2)
            return df["url"].dropna().astype(str).str.strip().head(top_n).tolist()
    except Exception:
        return []
    return []


def _load_recent_urls_multi(out_dir: str, ticker: str, lookback_days: int, top_n: int) -> list[str]:
    """
    최근 lookback_days 범위의 날짜 폴더에서 각 날짜의 상단 top_n개 URL만 모음.
    (파일 전체를 로드하지 않도록 url 컬럼만 부분 로드)
    """
    base = os.path.join(out_dir, ticker)
    if not os.path.exists(base):
        return []

    today = dt.date.today()
    items = []
    for name in os.listdir(base):
        p = os.path.join(base, name)
        if not os.path.isdir(p):
            continue
        try:
            d = dt.datetime.strptime(name, DATE_FMT).date()
        except ValueError:
            continue
        if (today - d).days <= lookback_days:
            items.append(d)
    if not items:
        return []

    items.sort(reverse=True)
    seen, st = [], set()
    for d in items:
        urls = _load_urls_head(os.path.join(base, d.strftime(DATE_FMT)), top_n=top_n)
        for u in urls:
            if u not in st:
                st.add(u)
                seen.append(u)
                if len(seen) >= top_n:   # 충분하면 즉시 종료
                    return seen
    return seen


# ───────────────────────────────────────────────────────────────────────────────
# Selenium 드라이버(리소스 최소화 설정)
# ───────────────────────────────────────────────────────────────────────────────
def _make_driver(user_agent: str | None = None):
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")

    use_remote = os.getenv("USE_REMOTE_WEBDRIVER", "false").lower() == "true"

    if use_remote:
        remote_url = os.getenv("SELENIUM_REMOTE_URL", "http://selenium:4444")
        return webdriver.Remote(command_executor=remote_url, options=opts)

    # 로컬 실행용 (컨테이너 안에 크롬 있을 때만)
    service = Service()
    return webdriver.Chrome(service=service, options=opts)


def _driver_get_with_retry(driver, url: str, tries: int = 2, sleep_sec: float = 2.0):
    last_err: Optional[Exception] = None
    for _ in range(tries):
        try:
            driver.get(url)
            return
        except (TimeoutException, WebDriverException) as e:
            last_err = e
        time.sleep(sleep_sec)
    raise last_err if last_err else TimeoutException(f"driver.get({url}) failed")


# ───────────────────────────────────────────────────────────────────────────────
# 링크 수집 (리소스 최소화): 스크롤 → 최종 page_source 한 번만 파싱
# ───────────────────────────────────────────────────────────────────────────────
def collect_yahoo_links_incremental(
    ticker: str,
    max_scroll: int,
    last_date: Optional[str],
    user_agent: str,
) -> List[Dict]:
    """
    리소스 최소화 버전:
    - 스크롤만 반복
    - 마지막에 단 1회 BeautifulSoup 파싱
    - last_date보다 과거 카드들은 목록에서 제외(스크롤 중단은 의미 적음)
    반환: [{'url': str, 'date_guess': 'YYYY-MM-DD' or None}, ...]
    """
    url = YF_BASE_NEWS_URL.format(ticker=ticker)
    driver = _make_driver(user_agent=user_agent)
    html = ""
    try:
        _driver_get_with_retry(driver, url, tries=int(os.environ.get("YF_GET_RETRIES", "2")))
        time.sleep(1.8)

        for _ in range(max_scroll):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)

        html = driver.page_source  # 최종본만 사용
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select('section[data-testid="storyitem"]')
    results, seen = [], set()

    for card in cards:
        a = card.find("a")
        href = a.get("href") if a else None
        if not href:
            continue
        link = href if href.startswith("http") else ("https://finance.yahoo.com" + href)
        if link in seen:
            continue
        seen.add(link)

        # 날짜 추정
        t = card.select_one("time[datetime]")
        guess = None
        if t and t.has_attr("datetime"):
            try:
                guess = t["datetime"].split("T")[0]
            except Exception:
                pass

        # 스크롤 완료 후 필터링(목록 축소)
        if last_date and guess and guess < last_date:
            continue

        results.append({"url": link, "date_guess": guess})

    # 메모리 해제 유도
    del soup, cards, html
    gc.collect()
    return results


# ───────────────────────────────────────────────────────────────────────────────
# 날짜별 저장(기존 url만 부분 로드 → 신규만 병합 → 저장)
# ───────────────────────────────────────────────────────────────────────────────
def _save_by_article_date(rows: List[Dict], ticker: str, out_dir: str) -> None:
    if not rows:
        return

    df_all = pd.DataFrame(rows)
    for c in ["date", "url", "title", "content", "status_code", "fetched_at", "run_id", "ticker"]:
        if c not in df_all.columns:
            df_all[c] = ""

    for date_str, df_day in df_all.groupby("date"):
        save_dir = os.path.join(out_dir, ticker, str(date_str))
        os.makedirs(save_dir, exist_ok=True)

        pq = os.path.join(save_dir, "news.parquet")
        csv = os.path.join(save_dir, "news.csv")

        # 기존 URL만 가볍게 읽어 중복 제거
        existed = set()
        try:
            if os.path.exists(pq):
                existed = set(pd.read_parquet(pq, columns=["url"])["url"].astype(str).map(_norm_url))
            elif os.path.exists(csv):
                existed = set(pd.read_csv(csv, usecols=["url"])["url"].astype(str).map(_norm_url))
        except Exception:
            existed = set()

        df_day = df_day.copy()
        df_day["url"] = df_day["url"].astype(str).map(_norm_url)
        df_new = df_day[~df_day["url"].isin(existed)]

        if df_new.empty:
            continue

        # 최종 저장 직전에만 전체 로드 1회
        if os.path.exists(pq):
            old = pd.read_parquet(pq)
        elif os.path.exists(csv):
            old = pd.read_csv(csv)
        else:
            old = pd.DataFrame()

        merged = pd.concat([old, df_new], ignore_index=True, sort=False) if not old.empty else df_new
        merged.drop_duplicates(subset=["url"], keep="first", inplace=True)

        try:
            merged.to_parquet(pq, index=False)
            print(f"[{ticker}] 저장 완료: {pq} (rows={len(merged)})")
        except Exception:
            merged.to_csv(csv, index=False)
            print(f"[{ticker}] Parquet 미지원으로 CSV 저장: {csv}")

        # 메모리 해제
        del old, merged, df_new, df_day
        gc.collect()


# ───────────────────────────────────────────────────────────────────────────────
# 메인 파이프라인 (폴백 Selenium OFF + 티커마다 GC + 리소스 요약)
# ───────────────────────────────────────────────────────────────────────────────
def run_yahoo_pipeline(
    tickers: List[str],
    run_date: Optional[str] = None,       # 로그 표기(저장은 기사 date)
    run_id: str = "local-dev",
    out_dir: str = RAW_DIR,
    max_scroll: int = 20,
    max_articles_per_ticker: int = 200,
    requests_ua_mode: str = "round_robin",
) -> None:
    """
    리소스 최소화 원칙:
    - 티커당 Selenium 1회(스크롤만), Soup 1회 파싱
    - 본문 수집은 requests만 사용(폴백 Selenium OFF)
    - 기존 파일은 url만 부분 로드해 중복 제거
    - 티커 경계마다 GC 강제 호출
    - 실행 후 리소스 요약 출력(psutil 있으면)
    """
    run_date = run_date or dt.datetime.now().strftime(DATE_FMT)

    with ResourceMonitor(label="crawling") as mon:
        for ticker in tickers:
            last_date = _last_date_dir(out_dir, ticker)  # "YYYY-MM-DD" or None
            stop_urls = _load_recent_urls_multi(
                out_dir=out_dir,
                ticker=ticker,
                lookback_days=STOP_LOOKBACK_DAYS,
                top_n=STOP_TOPN,
            )

            ua = random.choice(UA_LIST) if UA_LIST else ""
            items = collect_yahoo_links_incremental(
                ticker=ticker,
                max_scroll=max_scroll,
                last_date=last_date,
                user_agent=ua,
            )
            if not items:
                print(f"[{ticker}] 새 링크 없음. (run_date={run_date})")
                mon.tick(); gc.collect()
                continue

            # last_date 기반 1차 필터
            if last_date:
                items = [it for it in items if (it["date_guess"] is None) or (it["date_guess"] >= last_date)]
                if not items:
                    mon.tick(); gc.collect()
                    continue

            # stop set 중복 제거
            if stop_urls:
                stop_set = set(map(_norm_url, stop_urls))
                items = [it for it in items if _norm_url(it["url"]) not in stop_set]
                if not items:
                    mon.tick(); gc.collect()
                    continue

            # 상한 컷
            items = items[:max_articles_per_ticker]
            urls = [it["url"] for it in items]

            # 본문 수집: requests만 (폴백 Selenium OFF)
            articles = fetch_articles_http(
                urls=urls,
                ua_mode=requests_ua_mode,
                delay_range=(0.6, 1.1),
                min_len_for_ok=120,
                enable_selenium_fallback=False,
            )
            if not articles:
                mon.tick(); gc.collect()
                continue

            # 본문 date 확정 후 last_date 기준 최종 필터
            if last_date:
                articles = [a for a in articles if a.get("date") and a["date"] >= last_date]
                if not articles:
                    mon.tick(); gc.collect()
                    continue

            now_iso = dt.datetime.now().isoformat(timespec="seconds")
            rows: List[Dict] = [{
                "ticker": ticker,
                "url": a.get("url"),
                "title": a.get("title", ""),
                "date": a.get("date"),
                "content": a.get("content", ""),
                "status_code": a.get("status_code"),
                "fetched_at": now_iso,
                "run_id": run_id,
            } for a in articles]

            _save_by_article_date(rows, ticker=ticker, out_dir=out_dir)

            # 티커 종료 시 샘플/GC
            mon.tick()
            del items, urls, articles, rows, stop_urls
            gc.collect()

