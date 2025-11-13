"""
MySQL 데이터베이스 유틸리티
메타데이터 저장 및 조회 기능
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
from contextlib import contextmanager


class DBManager:
    """MySQL 데이터베이스 관리 클래스"""
    
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST', 'localhost')
        self.port = int(os.getenv('MYSQL_PORT', 3306))
        self.database = os.getenv('MYSQL_DATABASE', 'stockmind_db')
        self.user = os.getenv('MYSQL_USER', 'stockmind_user')
        self.password = os.getenv('MYSQL_PASSWORD', 'stockmind_password')
    
    @contextmanager
    def get_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    # ===== Crawling History =====
    
    def insert_crawling_start(
        self,
        run_id: str,
        ticker: str,
        crawl_date: str
    ) -> int:
        """크롤링 시작 기록"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO crawling_history 
                (run_id, ticker, crawl_date, status, started_at)
                VALUES (%s, %s, %s, 'running', NOW())
                """
                cursor.execute(sql, (run_id, ticker, crawl_date))
                return cursor.lastrowid
    
    def update_crawling_success(
        self,
        crawling_id: int,
        article_count: int,
        s3_path: str
    ):
        """크롤링 성공 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE crawling_history 
                SET status = 'success',
                    article_count = %s,
                    s3_raw_path = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
                cursor.execute(sql, (article_count, s3_path, crawling_id))
    
    def update_crawling_failed(
        self,
        crawling_id: int,
        error_message: str
    ):
        """크롤링 실패 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE crawling_history 
                SET status = 'failed',
                    error_message = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
                cursor.execute(sql, (error_message, crawling_id))
    
    # ===== Analysis History =====
    
    def insert_analysis_start(
        self,
        run_id: str,
        ticker: str,
        analysis_date: str,
        crawling_history_id: Optional[int] = None
    ) -> int:
        """분석 시작 기록"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO analysis_history 
                (run_id, ticker, analysis_date, crawling_history_id, status, started_at)
                VALUES (%s, %s, %s, %s, 'running', NOW())
                """
                cursor.execute(sql, (run_id, ticker, analysis_date, crawling_history_id))
                return cursor.lastrowid
    
    def update_analysis_success(
        self,
        analysis_id: int,
        s3_path: str,
        sentiment_stats: Dict[str, float],
        keyword_count: int
    ):
        """분석 성공 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE analysis_history 
                SET status = 'success',
                    s3_result_path = %s,
                    sentiment_avg = %s,
                    sentiment_positive_ratio = %s,
                    sentiment_negative_ratio = %s,
                    sentiment_neutral_ratio = %s,
                    keyword_count = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
                cursor.execute(sql, (
                    s3_path,
                    sentiment_stats.get('avg', 0),
                    sentiment_stats.get('positive_ratio', 0),
                    sentiment_stats.get('negative_ratio', 0),
                    sentiment_stats.get('neutral_ratio', 0),
                    keyword_count,
                    analysis_id
                ))
    
    def update_analysis_failed(
        self,
        analysis_id: int,
        error_message: str
    ):
        """분석 실패 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE analysis_history 
                SET status = 'failed',
                    error_message = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
                cursor.execute(sql, (error_message, analysis_id))
    
    # ===== DAG Run History =====
    
    def insert_dag_run(
        self,
        run_id: str,
        dag_id: str,
        execution_date: str,
        tickers: List[str]
    ) -> int:
        """DAG 실행 시작 기록"""
        import json
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO dag_run_history 
                (run_id, dag_id, execution_date, tickers, status, started_at)
                VALUES (%s, %s, %s, %s, 'running', NOW())
                ON DUPLICATE KEY UPDATE
                    status = 'running',
                    started_at = NOW()
                """
                cursor.execute(sql, (run_id, dag_id, execution_date, json.dumps(tickers)))
                return cursor.lastrowid
    
    def update_dag_run_success(self, run_id: str, total_articles: int):
        """DAG 실행 성공 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE dag_run_history 
                SET status = 'success',
                    total_articles = %s,
                    completed_at = NOW()
                WHERE run_id = %s
                """
                cursor.execute(sql, (total_articles, run_id))
    
    def update_dag_run_failed(self, run_id: str, error_message: str):
        """DAG 실행 실패 업데이트"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                UPDATE dag_run_history 
                SET status = 'failed',
                    error_message = %s,
                    completed_at = NOW()
                WHERE run_id = %s
                """
                cursor.execute(sql, (error_message, run_id))
    
    # ===== S3 File Metadata =====
    
    def insert_s3_metadata(
        self,
        file_type: str,
        ticker: str,
        file_date: str,
        s3_bucket: str,
        s3_key: str,
        file_size_bytes: int,
        row_count: Optional[int] = None
    ):
        """S3 파일 메타데이터 저장"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO s3_file_metadata 
                (file_type, ticker, file_date, s3_bucket, s3_key, 
                 file_size_bytes, row_count, uploaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    file_size_bytes = VALUES(file_size_bytes),
                    row_count = VALUES(row_count),
                    uploaded_at = NOW()
                """
                cursor.execute(sql, (
                    file_type, ticker, file_date, s3_bucket, s3_key,
                    file_size_bytes, row_count
                ))
    
    # ===== Query Methods =====
    
    def get_latest_crawl_date(self, ticker: str) -> Optional[str]:
        """특정 티커의 최근 크롤링 날짜 조회"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                SELECT MAX(crawl_date) as latest_date
                FROM crawling_history
                WHERE ticker = %s AND status = 'success'
                """
                cursor.execute(sql, (ticker,))
                result = cursor.fetchone()
                return result['latest_date'] if result else None
    
    def get_ticker_stats(self, ticker: str) -> Dict[str, Any]:
        """티커별 통계 조회"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = "SELECT * FROM v_ticker_stats WHERE ticker = %s"
                cursor.execute(sql, (ticker,))
                return cursor.fetchone() or {}
