from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class WatchlistPort(ABC):
    """
    HTS 관심종목 파일(CSV) 저장을 위한 포트 인터페이스.
    """

    @abstractmethod
    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터를 HTS 업로드용 CSV 파일로 저장합니다.
        """
        pass
