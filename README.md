# StockMind
## Predict Stock Price with [News/Chart Pattern]

```bash
StockMind/
├─ .env.sample                       # 환경변수 예시 파일 (.env로 복사 후 수정하여 사용)
├─ .gitignore                        # Git 추적 제외 설정
├─ README.md                         # 프로젝트 개요 및 사용 가이드
├─ code/
│  ├─ __init__.py                    # code/ 디렉토리 패키지 인식용
│  ├─ crawling/                      # Yahoo Finance 뉴스 크롤링 단계
│  │  ├─ __init__.py                # crawling/ 패키지 마커
│  │  ├─ settings.py                # 크롤링 설정 (UA, 경로, Selenium 옵션 등)
│  │  ├─ http_util.py               # UA 로테이션, 요청 세션 관리, 재시도 로직
│  │  ├─ yahoo_scraper.py           # Selenium으로 뉴스 링크 수집 (스크롤 기반)
│  │  ├─ article_fetcher.py         # requests 기반 기사 본문/제목/날짜 추출
│  │  ├─ crawling.py                # 전체 크롤링 파이프라인 (링크→본문→저장)
│  │  ├─ main_crawling.py           # 크롤링 엔트리포인트 (8개 티커 일괄 실행)
│  │  └─ ResourceMonitor(Class)     # 실행 시간·메모리·CPU 사용량 측정 도우미
│  ├─ analysis/                      # 뉴스 본문 분석 단계
│  │  ├─ __init__.py                # analysis/ 패키지 마커
│  │  ├─ settings.py                # 모델·파라미터·입출력 컬럼 설정
│  │  ├─ nlp_models.py              # Summarization / Sentiment / Keyword 모델 로딩
│  │  ├─ summarizer.py              # 본문 요약 (DistilBART 기반)
│  │  ├─ sentiment.py               # 감정 분류 (FinBERT 기반)
│  │  ├─ keywords.py                # 핵심 키워드 5개 추출 (KeyBERT)
│  │  ├─ processor.py               # 기사 단위 처리 및 중복 제거 로직
│  │  ├─ io_utils.py                # 입출력 유틸 (경로 생성, CSV 입출력, pending 판단)
│  │  └─ main_analysis.py           # 분석 단계 실행 스크립트 (CLI 실행 진입점)
│  └─ main.sh                       # 전체 실행용 쉘 스크립트 (예: crawling.main)
├─ data/
│  └─ raw/                          # 크롤링 결과 저장 루트
│     └─ {ticker}/
│        └─ {YYYY-MM-DD}/
│           └─ news.csv             # 기사 데이터 (url, title, content, date 등)
├─ docker-compose.yml                # (추가 예정) Docker 서비스 정의 (app + db + airflow)
├─ dockerfile                        # (추가 예정) Python 환경 정의 (모델/패키지 포함)
├─ requirements.txt                  # 의존 패키지 목록 (transformers, selenium 등)
├─ results/                          # 분석 결과 저장 루트
│  └─ {ticker}/
│     └─ {YYYY-MM-DD}.csv           # summary / sentiment / keywords 결과
└─ venv/                             # 로컬 가상환경 (Git 무시 대상)
```

