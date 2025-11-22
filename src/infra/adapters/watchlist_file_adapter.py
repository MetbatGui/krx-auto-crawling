# infra/adapters/watchlist_file_adapter.py
import pandas as pd
from typing import List, Dict

from core.ports.watchlist_port import WatchlistPort
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData


class WatchlistFileAdapter(WatchlistPort):
    """
    WatchlistPort의 구현체(Adapter).
    수집된 데이터에서 종목코드만 추출하여 HTS 업로드용 CSV 파일로 저장합니다.
    """
    
    def __init__(self, storage: StoragePort):
        """
        Args:
            storage: StoragePort 구현체 (LocalStorageAdapter 등)
        """
        self.storage = storage
        self.storage.ensure_directory("watchlist")
        print(f"[Adapter:WatchlistFile] 초기화 완료")

    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """
        당일 수집된 데이터를 HTS 업로드용 CSV 파일로 저장합니다.
        (모든 데이터의 종목코드를 순서대로 저장, 중복 포함)
        """
        if not data_list:
            print("  [Adapter:WatchlistFile] ⚠️ 데이터가 없어 관심종목 저장을 건너뜁니다.")
            return

        # 1. 모든 데이터에서 종목코드 수집 (중복 유지)
        all_codes = []
        date_str = data_list[0].date_str # 파일명용 날짜 (첫 번째 데이터 기준)

        for item in data_list:
            if not item.data.empty and '종목코드' in item.data.columns:
                all_codes.extend(item.data['종목코드'].tolist())

        if not all_codes:
            print("  [Adapter:WatchlistFile] ⚠️ 저장할 종목코드가 없습니다.")
            return

        # 2. DataFrame 생성 (헤더 포함, 종목코드 컬럼만)
        df_watchlist = pd.DataFrame({'종목코드': all_codes})
        
        # 3. StoragePort를 통해 저장
        filename = f"watchlist/{date_str}_watchlist.csv"
        success = self.storage.save_dataframe_csv(
            df_watchlist,
            path=filename,
            header=True,
            index=False,
            encoding='cp949'
        )
        
        if success:
           print(f"  [Adapter:WatchlistFile] ✅ 관심종목 파일 저장 완료: {date_str}_watchlist.csv ({len(df_watchlist)}개 종목)")
    
    def save_cumulative_watchlist(self, top_stocks: Dict[str, List[str]], date_str: str) -> None:
        """
        누적 상위 종목(마스터 리포트의 총계 기준 Top 20)을 CSV 파일로 저장합니다.
        """
        if not top_stocks:
            print("  [Adapter:WatchlistFile] ⚠️ 누적 상위종목 데이터가 없어 저장을 건너뜁니다.")
            return

        # 1. 모든 리포트의 상위 종목을 순서대로 수집 (중복 포함)
        all_stock_names = []
        for key in ['KOSPI_foreigner', 'KOSPI_institutions', 'KOSDAQ_foreigner', 'KOSDAQ_institutions']:
            if key in top_stocks:
                all_stock_names.extend(top_stocks[key])

        if not all_stock_names:
            print("  [Adapter:WatchlistFile] ⚠️ 저장할 누적 상위종목이 없습니다.")
            return

        # 2. DataFrame 생성 (종목명 컬럼)
        df_cumulative = pd.DataFrame({'종목명': all_stock_names})
        
        # 3. StoragePort를 통해 저장
        filename = f"watchlist/{date_str}_누적상위종목.csv"
        success = self.storage.save_dataframe_csv(
            df_cumulative,
            path=filename,
            header=True,
            index=False,
            encoding='cp949'
        )
        
        if success:
            print(f"  [Adapter:WatchlistFile] ✅ 누적 상위종목 파일 저장 완료: {date_str}_누적상위종목.csv ({len(df_cumulative)}개 종목)")