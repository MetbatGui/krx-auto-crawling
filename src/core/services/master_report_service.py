"""
마스터 리포트 비즈니스 로직 서비스

전체 워크플로우를 오케스트레이션하고 StoragePort를 통해 파일 I/O를 수행합니다.
"""
import pandas as pd
import datetime
from typing import Dict, List
from pathlib import Path

from core.ports.storage_port import StoragePort
from core.domain.models import KrxData


class MasterReportService:
    """마스터 리포트 비즈니스 로직 서비스"""
    
    def __init__(self, storage: StoragePort, file_name_prefix: str = "2025"):
        """
        Args:
            storage: 파일 저장/로드를 위한 StoragePort
            file_name_prefix: 파일명에 사용될 연도 접두사
        """
        self.storage = storage
        self.excel_columns = ['일자', '종목', '금액']
        
        # 파일 경로 설정
        self.master_subdir = "순매수도"
        year_suffix = f"({file_name_prefix})"
        self.file_map: Dict[str, str] = {
            'KOSPI_foreigner': f'코스피외국인순매수도{year_suffix}.xlsx',
            'KOSDAQ_foreigner': f'코스닥외국인순매수도{year_suffix}.xlsx',
            'KOSPI_institutions': f'코스피기관순매수도{year_suffix}.xlsx',
            'KOSDAQ_institutions': f'코스닥기관순매수도{year_suffix}.xlsx',
        }
        
        # 순매수도 디렉토리 생성
        self.storage.ensure_directory(self.master_subdir)
    
    def transform_to_excel_schema(
        self,
        daily_data: pd.DataFrame,
        date_int: int
    ) -> pd.DataFrame:
        """
        일별 데이터를 Excel 스키마로 변환합니다.
        
        Args:
            daily_data: KRX 일별 데이터 (종목명, 순매수_거래대금 컬럼 포함)
            date_int: 날짜 정수 (예: 20251121)
            
        Returns:
            변환된 DataFrame (일자, 종목, 금액 컬럼)
        """
        try:
            data_dict = {
                '일자': date_int,
                '종목': daily_data['종목명'],
                '금액': pd.to_numeric(daily_data['순매수_거래대금'])
            }
            
            formatted_df = pd.DataFrame(data_dict)
            formatted_df = formatted_df[self.excel_columns]
            
            print(f"    -> [Service:MasterReport] 데이터 변환 완료 ({len(formatted_df)}개 종목)")
            return formatted_df
            
        except Exception as e:
            print(f"    -> [Service:MasterReport] 🚨 데이터 변환 실패: {e}")
            raise
    
    def check_duplicate_date(
        self,
        existing_df: pd.DataFrame,
        date_int: int
    ) -> bool:
        """
        중복 날짜가 있는지 확인합니다.
        
        Args:
            existing_df: 기존 데이터 DataFrame
            date_int: 확인할 날짜 정수
            
        Returns:
            True if 중복 존재, False otherwise
        """
        if existing_df.empty:
            return False
        
        is_duplicate = date_int in existing_df['일자'].values
        
        if is_duplicate:
            print(f"    -> [Service:MasterReport] ⚠️ {date_int} 데이터 중복 발견")
        
        return is_duplicate
    
    def merge_data(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        기존 데이터와 신규 데이터를 병합합니다.
        
        Args:
            existing_df: 기존 데이터
            new_df: 신규 데이터
            
        Returns:
            병합된 DataFrame
        """
        if existing_df.empty:
            merged = new_df.copy()
        else:
            merged = pd.concat([existing_df, new_df], ignore_index=True)
        
        print(f"    -> [Service:MasterReport] 데이터 병합 완료 (총 {len(merged)}줄)")
        return merged
    
    def create_empty_dataframe(self) -> pd.DataFrame:
        """
        빈 Excel 스키마 DataFrame을 생성합니다.
        
        Returns:
            빈 DataFrame (일자, 종목, 금액 컬럼)
        """
        return pd.DataFrame(columns=self.excel_columns)
