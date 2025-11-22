from abc import ABC, abstractmethod
from typing import List
from core.domain.models import KrxData

class RankingReportPort(ABC):
    """
    수급 순위 리포트 업데이트를 위한 포트 인터페이스.
    """

    @abstractmethod
    def update_ranking_report(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터를 기반으로 수급 순위표를 업데이트합니다.
        """
        pass
