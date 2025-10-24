from abc import ABC, abstractmethod
import datetime
from typing import Dict, Set
import pandas as pd

class ExcelRankingReportPort(ABC):
    """
    '일별 수급 순위 정리표' 엑셀 파일을 업데이트하는
    어댑터(Adapter)가 구현해야 하는 Port(Interface).
    """

    @abstractmethod
    def update_ranking_report(
        self,
        report_date: datetime.date,
        previous_date: datetime.date,
        data_to_paste: Dict[str, pd.DataFrame],
        common_stocks: Dict[str, Set[str]]
    ) -> bool:
        """
        일별 수급 순위 정리표를 업데이트합니다.

        이 메서드의 구현체(Adapter)는 다음 작업을 수행해야 합니다:
        1. '2025일별수급순위정리표.xlsx' 파일을 엽니다.
        2. 'previous_date' (예: '1023') 시트를 'report_date' (예: '1024')로 복사합니다.
        3. 새 시트의 날짜/요일 셀(예: A1)을 'report_date' 기준으로 수정합니다.
        4. 'data_to_paste' (KOSPI/KOSDAQ 데이터)를 새 시트의 적절한 위치에 붙여넣습니다.
        5. 'common_stocks' (기관/외국인 공통 매수 종목) 세트를 기반으로
           해당 종목 셀에 배경색 서식을 적용합니다.
        6. 파일을 저장합니다.

        Args:
            report_date (datetime.date): 작업 대상 날짜 (오늘 날짜).
            previous_date (datetime.date): 복사할 원본이 되는 날짜 (어제 날짜).
            data_to_paste (Dict[str, pd.DataFrame]):
                붙여넣을 데이터. (예: {'KOSPI_f', df1, 'KOSPI_i', df2, ...})
            common_stocks (Dict[str, Set[str]]):
                배경색을 칠할 공통 종목 세트.
                (예: {'KOSPI': {'삼성전자', ...}, 'KOSDAQ': {'에코프로', ...}})

        Returns:
            bool: 업데이트 성공 여부.
        """
        pass