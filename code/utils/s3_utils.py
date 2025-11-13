"""
AWS S3 업로드 유틸리티
로컬 파일을 S3에 업로드하고 메타데이터 저장
"""

import os
from pathlib import Path
from typing import Optional
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


class S3Uploader:
    """S3 파일 업로드 관리 클래스"""
    
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'stockmind')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-2')
        
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
    
    def upload_file(
        self,
        local_path: str,
        s3_key: str,
        extra_args: Optional[dict] = None
    ) -> bool:
        """
        로컬 파일을 S3에 업로드
        
        Args:
            local_path: 업로드할 로컬 파일 경로
            s3_key: S3 객체 키 (경로)
            extra_args: 추가 업로드 옵션 (메타데이터 등)
        
        Returns:
            업로드 성공 여부
        """
        try:
            if extra_args is None:
                extra_args = {}
            
            # 기본 메타데이터 설정
            if 'Metadata' not in extra_args:
                extra_args['Metadata'] = {}
            
            extra_args['Metadata']['uploaded-by'] = 'stockmind-pipeline'
            
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Successfully uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_path}")
            return False
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            return False
    
    def upload_raw_news(
        self,
        ticker: str,
        date_str: str,
        local_csv_path: str
    ) -> Optional[str]:
        """
        Raw 뉴스 데이터를 S3에 업로드
        
        Args:
            ticker: 주식 티커
            date_str: 날짜 (YYYY-MM-DD)
            local_csv_path: 로컬 CSV 파일 경로
        
        Returns:
            S3 키 (경로) 또는 None
        """
        s3_key = f"raw/{ticker}/{date_str}/news.csv"
        
        if self.upload_file(local_csv_path, s3_key):
            return s3_key
        return None
    
    def upload_analysis_result(
        self,
        ticker: str,
        date_str: str,
        local_csv_path: str
    ) -> Optional[str]:
        """
        분석 결과를 S3에 업로드
        
        Args:
            ticker: 주식 티커
            date_str: 날짜 (YYYY-MM-DD)
            local_csv_path: 로컬 CSV 파일 경로
        
        Returns:
            S3 키 (경로) 또는 None
        """
        s3_key = f"results/{ticker}/{date_str}.csv"
        
        if self.upload_file(local_csv_path, s3_key):
            return s3_key
        return None
    
    def download_file(
        self,
        s3_key: str,
        local_path: str
    ) -> bool:
        """
        S3에서 파일 다운로드
        
        Args:
            s3_key: S3 객체 키
            local_path: 저장할 로컬 경로
        
        Returns:
            다운로드 성공 여부
        """
        try:
            # 로컬 디렉토리 생성
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                local_path
            )
            
            logger.info(f"Successfully downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"S3 download failed: {e}")
            return False
    
    def check_file_exists(self, s3_key: str) -> bool:
        """S3에 파일이 존재하는지 확인"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def get_file_size(self, s3_key: str) -> Optional[int]:
        """S3 파일 크기 조회 (bytes)"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return response['ContentLength']
        except ClientError:
            return None
    
    def list_files(self, prefix: str, max_keys: int = 1000) -> list:
        """
        특정 prefix로 시작하는 파일 목록 조회
        
        Args:
            prefix: S3 key prefix
            max_keys: 최대 조회 개수
        
        Returns:
            파일 목록 (dict list)
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                return response['Contents']
            return []
            
        except ClientError as e:
            logger.error(f"S3 list failed: {e}")
            return []


def get_local_file_size(file_path: str) -> int:
    """로컬 파일 크기 조회 (bytes)"""
    return os.path.getsize(file_path)


def count_csv_rows(csv_path: str) -> int:
    """CSV 파일의 행 개수 조회 (헤더 제외)"""
    import pandas as pd
    try:
        df = pd.read_csv(csv_path)
        return len(df)
    except Exception as e:
        logger.error(f"Failed to count CSV rows: {e}")
        return 0
