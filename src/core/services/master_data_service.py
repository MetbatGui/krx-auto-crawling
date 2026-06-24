"""
마스터 리포트 데이터 처리 서비스

데이터 변환, 병합, 피벗 계산 등 데이터 처리 책임만 담당
"""
import pandas as pd
from typing import List


class MasterDataService:
    """마스터 리포트 데이터 처리 전용 서비스.

    데이터 변환, 병합, 피벗 계산 등 순수 데이터 처리 로직을 담당합니다.
    """
    
    def __init__(self):
        """MasterDataService 초기화."""
        # 엑셀 스키마 정의 (종목코드 제외)
        self.excel_columns = ['일자', '종목', '금액']
    
    def transform_to_excel_schema(
        self,
        daily_data: pd.DataFrame,
        date_int: int
    ) -> pd.DataFrame:
        """일별 데이터를 Excel 스키마로 변환합니다.
        
        Args:
            daily_data (pd.DataFrame): KRX 일별 데이터 (종목코드, 종목명, 순매수_거래대금 컬럼 포함).
            date_int (int): 날짜 정수 (예: 20251121).
            
        Returns:
            pd.DataFrame: 변환된 DataFrame (일자, 종목, 금액 컬럼).
        """
        try:
            # 날짜 정수를 문자열 'YYYYMMDD'로 변환
            # date_int가 20251229 --> "20251229"
            date_str = str(date_int)
            
            formatted_df = (
                pd.DataFrame({
                    '일자': date_str,
                    # '종목코드': daily_data['종목코드'],  <-- 제외
                    '종목': daily_data['종목명'],
                    '금액': pd.to_numeric(daily_data['순매수_거래대금'])
                })
                [self.excel_columns]
            )
            
            print(f"    -> [Service:MasterData] 데이터 변환 완료 ({len(formatted_df)}개 종목)")
            return formatted_df
            
        except Exception as e:
            print(f"    -> [Service:MasterData] [Error] 데이터 변환 실패: {e}")
            raise
    
    def check_duplicate_date(
        self,
        existing_df: pd.DataFrame,
        date_int: int
    ) -> bool:
        """중복 날짜가 있는지 확인합니다.
        
        Args:
            existing_df (pd.DataFrame): 기존 데이터 DataFrame.
            date_int (int): 확인할 날짜 정수.
            
        Returns:
            bool: 중복 존재 시 True, 그렇지 않으면 False.
        """
        if existing_df.empty:
            return False
        
        # 날짜 정수를 문자열로 변환하여 비교
        try:
            target_date_str = str(date_int)
            # existing_df['일자']가 문자열인지, datetime인지, 숫자인지 확인 필요
            # 안전하게 문자열로 변환하여 비교
            existing_dates = existing_df['일자'].astype(str).values
            is_duplicate = target_date_str in existing_dates
            
            if is_duplicate:
                print(f"    -> [Service:MasterData] [Warn] {date_int} 데이터 중복 발견")
            
            return is_duplicate
        except Exception:
             return date_int in existing_df['일자'].values

    def merge_data(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """기존 데이터와 신규 데이터를 병합합니다.
        
        Args:
            existing_df (pd.DataFrame): 기존 데이터.
            new_df (pd.DataFrame): 신규 데이터.
            
        Returns:
            pd.DataFrame: 병합된 DataFrame.
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
        """피벗 테이블을 계산합니다.
        
        Args:
            data (pd.DataFrame): 원본 데이터 (일자, 종목, 금액 컬럼 포함).
            date_int (int): 기준 날짜 (피벗 컬럼에서 찾기 위함).
            
        Returns:
            pd.DataFrame: 정렬된 피벗 DataFrame (총계 포함).
        """
        if data.empty:
            print(f"    -> [Service:MasterData] [Warn] 데이터가 비어있어 피벗을 생성할 수 없습니다.")
            return pd.DataFrame()
        
        try:
            # 데이터 전처리 및 피벗 생성
            pivot = (
                data.assign(
                    금액=lambda x: pd.to_numeric(
                        x['금액'].astype(str).str.replace(r'[^0-9.-]', '', regex=True).replace('', 0),
                        errors='coerce'
                    ).fillna(0)
                )
                .pivot_table(
                    values='금액',
                    index='종목',
                    columns='일자',
                    aggfunc='sum'
                )
            )
            
            # 총계 계산 및 정렬
            pivot['총계'] = pivot.sum(axis=1)
            pivot_sorted = pivot.sort_values(by='총계', ascending=False)
            
            print(f"    -> [Service:MasterData] 피벗 테이블 계산 완료")
            return pivot_sorted
            
        except Exception as e:
            print(f"    -> [Service:MasterData] [Error] 피벗 계산 실패: {e}")
            return pd.DataFrame()
    
    def extract_top_stocks(
        self,
        pivot_data: pd.DataFrame,
        top_n: int = 30
    ) -> List[str]:
        """피벗 데이터에서 총계 기준 상위 N개 종목명을 추출합니다.
        
        Args:
            pivot_data (pd.DataFrame): 피벗 DataFrame (총계 컬럼 포함).
            top_n (int): 추출할 상위 종목 개수 (기본 30).
            
        Returns:
            List[str]: 상위 N개 종목명 리스트.
        """
        if pivot_data.empty or '총계' not in pivot_data.columns:
            print(f"    -> [Service:MasterData] [Warn] 피벗 데이터가 비어있거나 총계 컬럼이 없습니다")
            return []
        
        top_stocks = pivot_data.nlargest(top_n, '총계').index.tolist()
        print(f"    -> [Service:MasterData] Top {len(top_stocks)} 종목 추출 완료")
        
        return top_stocks
