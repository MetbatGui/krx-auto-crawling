from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class DailyReportPort(ABC):
    """
    일별 리포트(개별 엑셀 파일) 저장을 위한 포트 인터페이스.
    """

    @abstractmethod
    def save_daily_reports(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터 리스트를 각각의 일별 엑셀 파일로 저장합니다.
        """
        pass
