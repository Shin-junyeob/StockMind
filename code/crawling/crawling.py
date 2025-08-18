import os, random, datetime as dt

import pandas as pd


from .settings import RAW_DIR, UA_LIST, STOP_TOPN
from .yahoo_scraper import collect_yahoo_links
from .article_fetcher import fetch_articles_http


def _load_recent_urls(parquet_path: str, top_n: int = STOP_TOPN) -> set[str]:
    if not os.path.exists(parquet_path):
        # csv 폴백 체크
        csv_path = parquet_path.replace(".parquet", ".csv")
        if not os.path.exists(csv_path):
            return set()
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            return set()
    else:
        try:
            df = pd.read_parquet(parquet_path)
        except Exception:
            # parquet 실패시 csv 폴백
            csv_path = parquet_path.replace(".parquet", ".csv")
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
            else:
                return set()
    if df.empty or "url" not in df.columns:
        return set()
    return set(df["url"].head(top_n).tolist())

def run_yahoo_pipeline(
    tickers: list[str],
    run_date: str | None = None,           # None면 오늘
    run_id: str = "local-dev",
    out_dir: str = RAW_DIR,
    max_scroll: int = 20,
    max_articles_per_ticker: int = 200,
    requests_ua_mode: str = "round_robin",
) -> None:
    run_date = run_date or dt.datetime.now().strftime("%Y-%m-%d")

    for ticker in tickers:
        save_dir = os.path.join(out_dir, ticker, run_date)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, "news.parquet")

        stop_urls = _load_recent_urls(save_path)

        # 1) 링크 수집 (Selenium)
        links = collect_yahoo_links(
            ticker=ticker,
            max_scroll=max_scroll,
            stop_urls=stop_urls,
            user_agent=random.choice(UA_LIST),  # 세션 단위 1회 고정
        )
        if not links:
            print(f"[{ticker}] 새 링크 없음.")
            continue

        links = links[:max_articles_per_ticker]

        # 2) 본문 수집 (requests + UA 로테이션, 필요시 Selenium 폴백)
        articles = fetch_articles_http(
            urls=links,
            ua_mode=requests_ua_mode,
            delay_range=(0.6, 1.3),
            min_len_for_ok=120,
            enable_selenium_fallback=True,
        )

        # 3) 저장(누적 + 중복 제거)
        df_new = pd.DataFrame([
            {
                "ticker": ticker,
                "url": a.get("url"),
                "title": a.get("title", ""),
                "date": a.get("date"),
                "content": a.get("content", ""),
                "status_code": a.get("status_code"),
                "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
                "run_id": run_id,
            }
            for a in articles
        ])

        if os.path.exists(save_path):
            try:
                old = pd.read_parquet(save_path)
            except Exception:
                old_csv = save_path.replace(".parquet", ".csv")
                old = pd.read_csv(old_csv) if os.path.exists(old_csv) else pd.DataFrame()
            df_all = pd.concat([df_new, old], ignore_index=True)
        else:
            df_all = df_new

        if not df_all.empty:
            df_all.drop_duplicates(subset="url", keep="first", inplace=True)

        try:
            df_all.to_parquet(save_path, index=False)
            print(f"[{ticker}] 저장 완료: {save_path} (rows={len(df_all)})")
        except Exception:
            csv_path = save_path.replace(".parquet", ".csv")
            df_all.to_csv(csv_path, index=False)
            print(f"[{ticker}] Parquet 미지원으로 CSV 저장: {csv_path}")
            