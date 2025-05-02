#!/bin/bash

echo "📍 Step 1-3: 시각화 + 뉴스 수집 + 본문 수집 (venv)"
source ../venv/bin/activate
python 1st_stock_graph.py
python 2nd_create_csv_with_link.py
python 3rd_add_content_in_csv.py
deactivate

echo "📍 Step 4: 뉴스 전처리 (venv310)"
source ../venv310/bin/activate
python 4th_analysis.py
deactivate

echo "📍 Step 5: metadata 생성 (vent)"
source ../venv/bin/activate
python 5th_make_metadata.py
deactivate

echo "✅ 전체 파이프라인 실행 완료!"