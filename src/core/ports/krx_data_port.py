# core/ports/krx_data_port.py
from abc import ABC, abstractmethod
from typing import Optional

class KrxDataPort(ABC):
    """
    Core 비즈니스 로직(Task)이 KRX 원본 데이터에 접근하기 위한
    인터페이스(약속, Port)입니다.
    """
    
    @abstractmethod
    def fetch_net_value_data(
        self, 
        market: str, 
        investor: str, 
        date_str: Optional[str] = None
    ) -> bytes:
        """
        지정된 조건의 투자자별 순매수 원본 엑셀(bytes)을 가져옵니다.
        
        Core 로직(Task)은 이 메서드가 '어떻게' 구현되는지 
        (e.g., HTTP, OTP, cloudscraper) 알 필요가 없습니다.

        Args:
            market (str): 시장 구분 ('KOSPI', 'KOSDAQ')
            investor (str): 투자자 구분 ('institutions', 'foreigner')
            date_str (str, optional): YYYYMMDD 날짜. 
                None이면 구현체(Adapter)가 '오늘 날짜'로 처리해야 합니다.

        Returns:
            bytes: 원본 엑셀 파일(bytes)
            
        Raises:
            Exception: 데이터 수집 중 발생한 모든 예외 (Adapter가 처리)
        """
        pass