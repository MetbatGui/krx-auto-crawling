"""순위표 리포트 생성 포트 인터페이스"""

from abc import ABC, abstractmethod
from typing import Dict, Set
import pandas as pd
import datetime


class RankingReportPort(ABC):
    """순위표 리포트 생성을 위한 포트 인터페이스
    
    Excel, PDF, HTML 등 다양한 형식으로 순위표를 생성할 수 있도록
    추상화된 인터페이스를 제공합니다.
    """
    
    @abstractmethod
    def update_report(
        self,
        report_date: datetime.date,
        data_map: Dict[str, pd.DataFrame],
        common_stocks: Dict[str, Set[str]]
    ) -> bool:
        """순위표 리포트를 업데이트합니다.
        
        Args:
            report_date (datetime.date): 리포트 작성 날짜.
            data_map (Dict[str, pd.DataFrame]): 시장별 데이터 {key: DataFrame}.
            common_stocks (Dict[str, Set[str]]): 시장별 공통 종목 {'KOSPI': {...}, 'KOSDAQ': {...}}.
            
        Returns:
            bool: 업데이트 성공 여부.
        """
        pass
