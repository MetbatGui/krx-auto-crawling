from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class MasterReportPort(ABC):
    """
    마스터 리포트(월별 누적 및 피벗) 업데이트를 위한 포트 인터페이스.
    """

    @abstractmethod
    def update_master_reports(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터를 마스터 파일에 누적하고 피벗 테이블을 갱신합니다.
        """
        pass
