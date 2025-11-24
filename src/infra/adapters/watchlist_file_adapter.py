"""Watchlist 파일 저장 어댑터"""

import pandas as pd
from typing import List, Dict
"""Watchlist 파일 저장 어댑터"""

import pandas as pd
from typing import List, Dict

from core.ports.watchlist_port import WatchlistPort
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData


class WatchlistFileAdapter(WatchlistPort):
    """WatchlistPort 구현체.

    일별/누적 상위 종목을 HTS 업로드용 CSV 파일로 저장합니다.

    Attributes:
        storage (StoragePort): 파일 저장 포트
    """
    
    REPORT_ORDER = ['KOSPI_foreigner', 'KOSDAQ_foreigner', 'KOSPI_institutions', 'KOSDAQ_institutions']
    TOP_N = 20
    
    def __init__(self, storage: StoragePort):
        """WatchlistFileAdapter 초기화.

        Args:
            storage: StoragePort 구현체
        """
        self.storage = storage
        self.storage.ensure_directory("watchlist")
        print(f"[Adapter:WatchlistFile] 초기화 완료")

    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """일별 상위 종목을 CSV 파일로 저장합니다.
        
        각 리포트별 상위 20개씩, 총 80개 종목을 저장합니다.
        순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        
        Args:
            data_list: KRX 데이터 리스트
        """
        if not data_list:
            print("  [Adapter:WatchlistFile] ⚠️ 데이터가 없어 저장을 건너뜁니다")
            return

        date_str = data_list[0].date_str
        
        # 각 리포트별 상위 20개 종목명 추출
        top_stocks_map = {}
        for item in data_list:
            if item.data.empty or '종목명' not in item.data.columns:
                continue
            top_stocks_map[item.key] = item.data['종목명'].head(self.TOP_N).tolist()
        
        if not top_stocks_map:
            print("  [Adapter:WatchlistFile] ⚠️ 저장할 종목이 없습니다")
            return
        
        # 공통 저장 로직 사용
        self._save_stock_list(
            top_stocks_map,
            date_str,
            f"{date_str}_일별상위종목.csv",
            "일별 상위종목"
        )
    
    def save_cumulative_watchlist(self, top_stocks: Dict[str, List[str]], date_str: str) -> None:
        """누적 상위 종목을 CSV 파일로 저장합니다.
        
        마스터 리포트의 총계 기준 상위 20개씩, 총 80개 종목을 저장합니다.
        순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        
        Args:
            top_stocks: 리포트별 상위 종목 딕셔너리
            date_str: 날짜 문자열
        """
        if not top_stocks:
            print("  [Adapter:WatchlistFile] ⚠️ 누적 상위종목 데이터가 없어 저장을 건너뜁니다")
            return
        
        # 공통 저장 로직 사용
        self._save_stock_list(
            top_stocks,
            date_str,
            f"{date_str}_누적상위종목.csv",
            "누적 상위종목"
        )
    
    def _save_stock_list(
        self,
        top_stocks: Dict[str, List[str]],
        date_str: str,
        filename: str,
        description: str
    ) -> None:
        """종목 리스트를 CSV 파일로 저장하는 공통 로직.
        
        Args:
            top_stocks: 리포트별 종목 딕셔너리
            date_str: 날짜 문자열
            filename: 저장할 파일명
            description: 로그용 설명
        """
        # 정해진 순서대로 종목 수집 (80개)
        # 순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        all_stock_names = []
        for key in self.REPORT_ORDER:
            if key in top_stocks:
                all_stock_names.extend(top_stocks[key])
        
        if not all_stock_names:
            print(f"  [Adapter:WatchlistFile] ⚠️ 저장할 {description}이 없습니다")
            return
        
        # DataFrame 생성 (헤더: 종목명)
        df = pd.DataFrame({'종목명': all_stock_names})
        
        # 저장
        file_path = f"watchlist/{filename}"
        success = self.storage.save_dataframe_csv(
            df,
            path=file_path,
            header=True,
            index=False,
            encoding='cp949'
        )
        
        if success:
            print(f"  [Adapter:WatchlistFile] ✅ {description} 파일 저장 완료: {filename} ({len(df)}개 종목)")