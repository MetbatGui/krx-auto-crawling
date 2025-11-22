"""
마스터 리포트 비즈니스 로직 서비스

MasterExcelAdapter에서 분리된 데이터 처리 및 검증 로직을 제공합니다.
"""
import pandas as pd
from infra.adapters.excel import PivotTableCalculator


class MasterReportService:
    """마스터 리포트 데이터 처리 서비스"""
    
    def __init__(self, pivot_calculator: PivotTableCalculator):
        """
        Args:
            pivot_calculator: 피벗 계산 유틸리티
        """
        self.pivot_calculator = pivot_calculator
        self.excel_columns = ['일자', '종목', '금액']
    
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
