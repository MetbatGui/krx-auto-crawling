"""
마스터 리포트 데이터 처리 서비스

데이터 변환, 병합, 피벗 계산 등 데이터 처리 책임만 담당
"""
import pandas as pd
from typing import List


class MasterDataService:
    """마스터 리포트 데이터 처리 전용 서비스"""
    
    def __init__(self):
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
            
            print(f"    -> [Service:MasterData] 데이터 변환 완료 ({len(formatted_df)}개 종목)")
            return formatted_df
            
        except Exception as e:
            print(f"    -> [Service:MasterData] 🚨 데이터 변환 실패: {e}")
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
            print(f"    -> [Service:MasterData] ⚠️ {date_int} 데이터 중복 발견")
        
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
        
        print(f"    -> [Service:MasterData] 데이터 병합 완료 (총 {len(merged)}줄)")
        return merged
    
    def calculate_pivot(
        self, 
        data: pd.DataFrame, 
        date_int: int
    ) -> pd.DataFrame:
        """
        피벗 테이블을 계산합니다.
        
        Args:
            data: 원본 데이터 (일자, 종목, 금액 컬럼 포함)
            date_int: 기준 날짜 (피벗 컬럼에서 찾기 위함)
            
        Returns:
            정렬된 피벗 DataFrame (총계 포함)
        """
        if data.empty:
            print(f"    -> [Service:MasterData] ⚠️ 데이터가 비어있어 피벗을 생성할 수 없습니다.")
            return pd.DataFrame()
        
        try:
            # 1. 금액 컬럼 정제 (문자열 -> 숫자)
            data = data.copy()
            data['금액'] = data['금액'].astype(str).str.replace(r'[^0-9.-]', '', regex=True)
            data['금액'] = data['금액'].replace('', 0)
            data['금액'] = pd.to_numeric(data['금액'], errors='coerce').fillna(0)
            
            # 2. 피벗 테이블 생성
            pivot = pd.pivot_table(
                data,
                values='금액',
                index='종목',
                columns='일자',
                aggfunc='sum'
            )
            
            # 3. 총계 추가 및 정렬
            pivot['총계'] = pivot.sum(axis=1)
            pivot_sorted = pivot.sort_values(by='총계', ascending=False)
            
            print(f"    -> [Service:MasterData] 피벗 테이블 계산 완료")
            return pivot_sorted
            
        except Exception as e:
            print(f"    -> [Service:MasterData] 🚨 피벗 계산 실패: {e}")
            return pd.DataFrame()
