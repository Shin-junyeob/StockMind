import os

# root directory
BASE_DIR = os.getenv("STOCKMIND_BASE_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# 모델 이름
SUMMARIZER_MODEL = "sshleifer/distilbart-cnn-12-6"
SENTIMENT_MODEL = "ProsusAI/finbert"

# 요약 파라미터
CHUNK_CHARS = 1100
SUM_MAX_LEN = 130
SUM_MIN_LEN = 30

# 키워드 추출
KEYWORDS_TOPK = 5

# 병합 정책
DEDUP_SUBSET = ["summary"] # summary 기준 중복 제거

# 입출력 파일명
INPUT_FILENAME = "news.csv" # data/raw/{ticker}/{date}/news.csv

# 결과 컬럼 고정
OUTPUT_COLUMNS = ["summary", "sentiment", "keywords"]
