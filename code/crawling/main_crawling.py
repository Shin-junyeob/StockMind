import os

from .crawling import run_yahoo_pipeline
from . import settings

if __name__ == "__main__":
    tickers = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN" ,"NVDA", "NFLX"]
    run_yahoo_pipeline(
        tickers=tickers,
        run_id=os.getenv("RUN_ID", "batch-8tickers"),
        out_dir=os.getenv("STOCKMIND_DATA_DIR", settings.RAW_DIR),
        max_scroll=int(os.getenv("YF_MAX_SCROLL", "18")),
        max_articles_per_ticker=int(os.getenv("YF_MAX_ARTICLES", "120")),
        requests_ua_mode=os.getenv("UA_MODE", "round_robin"),
    )
