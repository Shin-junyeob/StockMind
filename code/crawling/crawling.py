import os, random, datetime as dt
from datetime import datetime, timedelta
import pandas as pd

from .settings import RAW_DIR, UA_LIST, STOP_TOPN, STOP_LOOKBACK_DAYS
from .yahoo_scraper import collect_yahoo_links
from .article_fetcher import fetch_articles_http


# ─────────────────────────────────────────────────────────────────────────────
# 기존: run_date 폴더 하나만 보던 stop set → 변경: 최근 며칠치 폴더에서 URL 취합
# ─────────────────────────────────────────────────────────────────────────────
def _load_recent_urls_multi(out_dir: str, ticker: str,
                            lookback_days: int = 7,
                            top_n: int = STOP_TOPN) -> set[str]:
    """
    data/raw/{ticker}/{YYYY-MM-DD}/news.* 파일들을 최근 lookback_days만큼 훑어
    상단 URL들을 모아 stop set을 구성.
    - 너무 커지면(top_n * 3) 조기 반환
    """
    base = os.path.join(out_dir, ticker)
    if not os.path.isdir(base):
        return set()

    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    date_dirs = sorted(
        [d for d in os.listdir(base)
         if os.path.isdir(os.path.join(base, d)) and d >= cutoff],
        reverse=True
    )

    stop: set[str] = set()
    for d in date_dirs:
        pdir = os.path.join(base, d)
        for ext in ("parquet", "csv"):
            p = os.path.join(pdir, f"news.{ext}")
            if not os.path.exists(p):
                continue
            try:
                df = pd.read_parquet(p) if ext == "parquet" else pd.read_csv(p)
            except Exception:
                continue
            if df.empty or "url" not in df.columns:
                continue
            stop.update(df["url"].head(top_n).tolist())
            if len(stop) >= top_n * 3:
                return stop
    return stop


# ─────────────────────────────────────────────────────────────────────────────
# 기존: 실행일(run_date) 폴더 하나에 합쳐 저장 → 변경: 기사 date별로 분기 저장
# ─────────────────────────────────────────────────────────────────────────────
def _save_by_article_date(rows: list[dict], ticker: str, out_dir: str) -> None:
    """
    rows(DataFrame화 가능)를 기사 게시일(date) 기준으로 그룹핑해서
    data/raw/{ticker}/{date}/news.parquet(없으면 csv)로 저장(누적 + 중복 제거).
    """
    if not rows:
        return

    df_all = pd.DataFrame(rows)
    if df_all.empty:
        return

    # 스키마 안전장치
    keep_cols = ["ticker", "url", "title", "date", "content",
                 "status_code", "fetched_at", "run_id"]
    for c in keep_cols:
        if c not in df_all.columns:
            df_all[c] = None
    df_all = df_all[keep_cols]

    # date 누락(파싱 실패) 행은 오늘자로 보정
    today = dt.datetime.now().strftime("%Y-%m-%d")
    df_all["date"] = df_all["date"].fillna(today).replace("", today)

    for date_str, df_day in df_all.groupby("date"):
        save_dir = os.path.join(out_dir, ticker, str(date_str))
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, "news.parquet")

        # 기존 파일과 병합
        if os.path.exists(save_path):
            try:
                old = pd.read_parquet(save_path)
            except Exception:
                old_csv = save_path.replace(".parquet", ".csv")
                old = pd.read_csv(old_csv) if os.path.exists(old_csv) else pd.DataFrame()
            df_day = pd.concat([df_day, old], ignore_index=True)

        # URL 기준 중복 제거(최신 우선 keep='first')
        df_day.drop_duplicates(subset="url", keep="first", inplace=True)

        try:
            df_day.to_parquet(save_path, index=False)
            print(f"[{ticker}] 저장 완료: {save_path} (rows={len(df_day)})")
        except Exception:
            csv_path = save_path.replace(".parquet", ".csv")
            df_day.to_csv(csv_path, index=False)
            print(f"[{ticker}] Parquet 미지원으로 CSV 저장: {csv_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 메인 파이프라인
# ─────────────────────────────────────────────────────────────────────────────
def run_yahoo_pipeline(
    tickers: list[str],
    run_date: str | None = None,           # 로그 표기에만 사용(저장에는 기사 date 사용)
    run_id: str = "local-dev",
    out_dir: str = RAW_DIR,
    max_scroll: int = 20,
    max_articles_per_ticker: int = 200,
    requests_ua_mode: str = "round_robin",
) -> None:
    run_date = run_date or dt.datetime.now().strftime("%Y-%m-%d")

    for ticker in tickers:
        # 0) stop set: 최근 며칠치 날짜 폴더에서 상단 URL을 취합
        stop_urls = _load_recent_urls_multi(
            out_dir=out_dir,
            ticker=ticker,
            lookback_days=STOP_LOOKBACK_DAYS,
            top_n=STOP_TOPN
        )

        # 1) 링크 수집 (Selenium)
        links = collect_yahoo_links(
            ticker=ticker,
            max_scroll=max_scroll,
            stop_urls=stop_urls,
            user_agent=random.choice(UA_LIST),  # 세션 단위 1회 고정
        )
        if not links:
            print(f"[{ticker}] 새 링크 없음. (run_date={run_date})")
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

        # 3) 저장: 실행일 폴더가 아닌, 기사 게시일(date)별로 분기 저장
        rows = [
            {
                "ticker": ticker,
                "url": a.get("url"),
                "title": a.get("title", ""),
                "date": a.get("date"),                 # ← 기사 게시일 기준
                "content": a.get("content", ""),
                "status_code": a.get("status_code"),
                "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
                "run_id": run_id,
            }
            for a in articles
        ]
        _save_by_article_date(rows, ticker=ticker, out_dir=out_dir)
