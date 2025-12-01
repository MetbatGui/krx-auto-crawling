from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class WatchlistPort(ABC):
    """HTS 관심종목 파일(CSV) 저장을 위한 포트 인터페이스."""

    @abstractmethod
    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """수집된 데이터를 HTS 업로드용 CSV 파일로 저장합니다.

        Args:
            data_list (List[KrxData]): 저장할 KRX 데이터 리스트.
        """
    @abstractmethod
    def save_cumulative_watchlist(self, top_stocks_map: dict, date_str: str) -> None:
        """누적 상위 종목을 관심종목 파일로 저장합니다.

        Args:
            top_stocks_map (dict): 리포트 키별 상위 종목 리스트 맵.
            date_str (str): 날짜 문자열 (YYYYMMDD).
        """
        pass
