from typing import Any, Dict, List, Optional, TypedDict
import pandas as pd

from core.tasks.base_task import Task
from core.tasks.krx_net_value.standardize_data import StandardizeDataTaskOutput

# 1. 입력 TypedDict (Task 2의 Output)
class ProcessWatchlistTaskInput(StandardizeDataTaskOutput):
    pass

# 2. 출력 TypedDict
class ProcessWatchlistTaskOutput(TypedDict):
    date_str: Optional[str]
    status: str
    watchlist_df: Optional[pd.DataFrame]
    message: Optional[str]


class ProcessWatchlistTask(Task):
    """
    Standardize Task의 결과(processed_dfs_dict)를 받아 
    Watchlist DataFrame을 생성합니다. (순수 가공 책임)
    '종목명' 단일 컬럼과 콤마 구분을 위한 빈 컬럼으로 구성됩니다.
    """

    def __init__(self):
        # 정렬 순서 정의
        self.sort_order = [
            'KOSPI_foreigner',
            'KOSDAQ_foreigner',
            'KOSPI_institutions',
            'KOSDAQ_institutions',
        ]

    def execute(self, context: ProcessWatchlistTaskInput) -> ProcessWatchlistTaskOutput:
        """Task 실행: DF 정렬, '종목명' 추출 및 수직 결합 (중복 허용)"""
        
        print(f"--- [Task] {self.__class__.__name__} 시작 (Process Watchlist) ---")
        
        date_str = context.get('date_str')
        processed_dfs_dict = context.get('processed_dfs_dict')

        if context.get('status') in ('error', 'skipped') or not processed_dfs_dict:
            print("  -> 🚨 이전 Task(Standardize)가 실패했거나 DF 딕셔너리가 없습니다.")
            return ProcessWatchlistTaskOutput(
                date_str=date_str,
                status='skipped',
                watchlist_df=None,
                message='이전 Task 실패로 건너뜀'
            )

        stock_names_list: List[pd.Series] = []
        
        print("  -> Watchlist 결합 순서:")
        for key in self.sort_order:
            df = processed_dfs_dict.get(key)
            
            if df is None or df.empty:
                print(f"      - ⚠️ {key}: 데이터가 없어 건너뜁니다.")
                continue

            if '종목명' not in df.columns:
                print(f"      - 🚨 {key}: '종목명' 컬럼이 없습니다.")
                continue
                
            print(f"      - {key} (종목 {len(df)}개)")
            stock_names_list.append(df['종목명'])

        if not stock_names_list:
            return ProcessWatchlistTaskOutput(
                date_str=date_str,
                status='error',
                watchlist_df=None,
                message='Watchlist로 추출할 종목명이 없음'
            )

        # 4. 최종 DataFrame 생성 (중복 허용)
        final_series = pd.concat(stock_names_list, ignore_index=True)
        final_df = final_series.to_frame(name='종목명')

        # [!!!] 콤마 포함을 위해 빈 컬럼 추가
        # 헤더를 '종목명,'로 만들고, 각 행을 '현대로템,'로 만듭니다.
        final_df[''] = '' 
        
        print(f"  -> [Task] 총 {len(final_df)}개 종목명 Watchlist 생성 완료 (빈 컬럼 추가).")

        return ProcessWatchlistTaskOutput(
            date_str=date_str,
            status='success',
            watchlist_df=final_df,
            message='Watchlist 생성 완료'
        )