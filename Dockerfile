# StockMind 공용 베이스 이미지
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 타임존 및 기본 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Seoul

WORKDIR /app

# 파이썬 의존성 설치
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 코드/데이터는 docker-compose에서 마운트할 예정이라 COPY 생략
# ENV 설정
ENV STOCKMIND_BASE_DIR=/app \
    STOCKMIND_DATA_DIR=/app/data

CMD ["bash"]

