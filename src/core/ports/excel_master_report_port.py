# core/ports/excel_master_report_port.py (신규 생성)

from abc import ABC, abstractmethod
from typing import Protocol
import pandas as pd
import datetime

class ExcelMasterReportPort(Protocol):
    """
    '마스터(누적)' 엑셀 리포트 파일을 읽고, 수정하고, 저장하는
    복잡한 I/O 책임을 정의하는 Port(약속)입니다.
    """

    @abstractmethod
    def update_report(
        self,
        report_key: str,
        daily_data: pd.DataFrame,
        report_date: datetime.date
    ) -> bool:
        """
        주어진 일일 데이터를 기존 마스터 리포트 파일에 누적합니다.

        Args:
            report_key (str): 'KOSPI_foreigner' 등 리포트 종류 키
            daily_data (pd.DataFrame): 추가할 일일 데이터 (Standardize Task 결과)
            report_date (datetime.date): 데이터의 기준 날짜 (시트명 생성에 사용)

        Returns:
            bool: 성공 여부
        """
        ...