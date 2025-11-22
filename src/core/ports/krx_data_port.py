from abc import ABC, abstractmethod
from typing import Optional
from core.domain.models import Market, Investor

class KrxDataPort(ABC):
    """
    KRX 원본 데이터 수집을 위한 포트 인터페이스.
    """
    
    @abstractmethod
    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """
        지정된 조건의 투자자별 순매수 원본 엑셀(bytes)을 가져옵니다.
        """
        pass