from abc import ABC, abstractmethod
from typing import Optional
from core.domain.models import Market, Investor

class KrxDataPort(ABC):
    """KRX 원본 데이터 수집을 위한 포트 인터페이스."""
    
    @abstractmethod
    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """지정된 조건의 투자자별 순매수 원본 엑셀(bytes)을 가져옵니다.

        Args:
            market (Market): 대상 시장 (KOSPI, KOSDAQ).
            investor (Investor): 대상 투자자 (기관, 외국인).
            date_str (Optional[str]): 대상 날짜 (YYYYMMDD). None일 경우 구현체에 따라 처리(보통 오늘).

        Returns:
            bytes: 다운로드된 엑셀 파일의 바이너리 데이터.
        """
        pass