from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class DailyReportPort(ABC):
    """일별 리포트(개별 엑셀 파일) 저장을 위한 포트 인터페이스."""

    @abstractmethod
    def save_daily_reports(self, data_list: List[KrxData]) -> None:
        """수집된 데이터 리스트를 각각의 일별 엑셀 파일로 저장합니다.

        Args:
            data_list (List[KrxData]): 저장할 KRX 데이터 리스트.
        """
        pass

    @abstractmethod
    def load_daily_reports(self, date_str: str) -> List[KrxData]:
        """해당 날짜의 일별 리포트 파일들을 로드합니다.

        Args:
            date_str (str): 날짜 문자열 (YYYYMMDD).

        Returns:
            List[KrxData]: 로드된 KRX 데이터 리스트. 파일이 없거나 불완전하면 빈 리스트 반환.
        """
        pass
