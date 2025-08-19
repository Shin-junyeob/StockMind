import os

# root directory
BASE_DIR = os.getenv("STOCKMIND_BASE_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

DATA_DIR = os.getenv("STOCKMIND_DATA_DIR", os.path.join(BASE_DIR, "data"))
RAW_DIR = os.path.join(DATA_DIR, "raw")
RESULTS_DIR = os.path.join(DATA_DIR, "results")
DEFAULT_SCROLL = int(os.getenv("YF_MAX_SCROLL", "20"))
DEFAULT_ARTICLES = int(os.getenv("YF_MAX_ARTICLES", "200"))

STOP_TOPN = int(os.getenv("YF_STOP_TOPN", "15"))
STOP_LOOKBACK_DAYS = int(os.getenv("YF_STOP_LOOKBACK_DAYS", "7"))

# UA 3종(+1)과 공통 설정
UA_LIST = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Safari (iPhone)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    # Chrome (Linux)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

ACCEPT_LANGUAGE = "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
REQUEST_TIMEOUT = 15
TOTAL_RETRY = 3
BACKOFF_FACTOR = 0.8

# Selenium 공통 옵션
SELENIUM = {
    "headless": True,
    "disable_gpu": True,
    "window_size": (1920, 1080),
    "page_load_timeout": 180,
    "scroll_pause": 1.6,
    "max_stable_rounds": 2,
}
