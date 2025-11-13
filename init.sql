-- StockMind Database Initialization Script
-- Create metadata tables for tracking crawling and analysis

USE stockmind_db;

-- 크롤링 실행 이력 테이블
CREATE TABLE IF NOT EXISTS crawling_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    crawl_date DATE NOT NULL,
    article_count INT DEFAULT 0,
    s3_raw_path VARCHAR(500),
    status ENUM('pending', 'running', 'success', 'failed') DEFAULT 'pending',
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ticker_date (ticker, crawl_date),
    INDEX idx_run_id (run_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 분석 실행 이력 테이블
CREATE TABLE IF NOT EXISTS analysis_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    analysis_date DATE NOT NULL,
    crawling_history_id INT,
    s3_result_path VARCHAR(500),
    sentiment_avg DECIMAL(5,4),
    sentiment_positive_ratio DECIMAL(5,4),
    sentiment_negative_ratio DECIMAL(5,4),
    sentiment_neutral_ratio DECIMAL(5,4),
    keyword_count INT DEFAULT 0,
    status ENUM('pending', 'running', 'success', 'failed') DEFAULT 'pending',
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (crawling_history_id) REFERENCES crawling_history(id) ON DELETE SET NULL,
    INDEX idx_ticker_date (ticker, analysis_date),
    INDEX idx_run_id (run_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- DAG 실행 이력 테이블 (전체 파이프라인 추적)
CREATE TABLE IF NOT EXISTS dag_run_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL UNIQUE,
    dag_id VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    tickers TEXT,  -- JSON array of tickers
    total_articles INT DEFAULT 0,
    status ENUM('pending', 'running', 'success', 'failed', 'partial') DEFAULT 'pending',
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_run_id (run_id),
    INDEX idx_dag_id (dag_id),
    INDEX idx_execution_date (execution_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- S3 파일 메타데이터 테이블
CREATE TABLE IF NOT EXISTS s3_file_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_type ENUM('raw', 'result') NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    file_date DATE NOT NULL,
    s3_bucket VARCHAR(100) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    file_size_bytes BIGINT,
    row_count INT,
    uploaded_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ticker_date (ticker, file_date),
    INDEX idx_file_type (file_type),
    INDEX idx_s3_key (s3_key(255)),
    UNIQUE KEY unique_file (file_type, ticker, file_date, s3_key(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 초기 데이터 확인을 위한 뷰
CREATE OR REPLACE VIEW v_latest_runs AS
SELECT 
    ch.ticker,
    ch.crawl_date,
    ch.article_count,
    ch.status as crawl_status,
    ah.sentiment_avg,
    ah.status as analysis_status,
    ch.created_at as crawl_time,
    ah.created_at as analysis_time
FROM crawling_history ch
LEFT JOIN analysis_history ah ON ch.id = ah.crawling_history_id
ORDER BY ch.created_at DESC
LIMIT 100;

-- 티커별 통계 뷰
CREATE OR REPLACE VIEW v_ticker_stats AS
SELECT 
    ticker,
    COUNT(DISTINCT crawl_date) as total_crawl_days,
    SUM(article_count) as total_articles,
    AVG(article_count) as avg_articles_per_day,
    MAX(crawl_date) as last_crawl_date,
    COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count
FROM crawling_history
GROUP BY ticker;

-- 초기 데이터 삽입 (선택사항)
INSERT INTO dag_run_history (run_id, dag_id, execution_date, tickers, status)
VALUES ('init-run', 'stock_news_pipeline', CURDATE(), '["AAPL","GOOG","NVDA","TSLA","META","AMZN","NFLX","MSFT"]', 'pending')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;
