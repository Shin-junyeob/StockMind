# StockMind
## Predict Stock Price with [News/Chart Pattern]

```bash
StockMind/
├─ code/
│  ├─ main.sh                         # python -m crawling.main 실행용 쉘 스크립트(엔트리포인트)
│  ├─ __init__.py                     # code/ 를 패키지로 인식시키기 위한 파일
│  └─ crawling/
│     ├─ __init__.py                  # crawling/ 패키지 마커
│     ├─ settings.py                  # 경로/크롤링 파라미터/UA 등 공통 설정(.env 반영)
│     ├─ http_utils.py                # UA 로테이션, requests 세션/재시도 유틸
│     ├─ yahoo_scraper.py             # (Selenium) 티커별 뉴스 목록 스크롤/링크 수집
│     ├─ article_fetcher.py           # (requests) 기사 본문/제목/날짜 파싱 (+선택적 Selenium 폴백)
│     ├─ crawling.py                  # 오케스트레이션: 링크→본문→중복제거→저장(Parquet/CSV)
│     └─ main.py                      # 예시 실행/배치 실행 진입점(티커 목록 받아 파이프라인 실행)
├─ data/
│  └─ raw/                            # 결과 저장 루트 (예: data/raw/AAPL/2025-08-18/news.parquet)
└─ .env.sample                        # 환경변수 예시(경로/파라미터 가이드) -> 본인 환경에 맞게 변경하여 .env로 저장
```