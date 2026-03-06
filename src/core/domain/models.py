from enum import Enum
from dataclasses import dataclass
import pandas as pd
from typing import Optional

class Market(Enum):
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"

class Investor(Enum):
    INSTITUTIONS = "institutions"
    FOREIGNER = "foreigner"

@dataclass
class RawNetbuyData:
    """KRX 원본 바이트 데이터를 캡슐화하는 도메인 모델.
    
    서버에서 방금 다운로드하여 아직 파싱이나 필터링을 거치지 않은 순수한 날것(Raw) 상태입니다.
    
    Attributes:
        market (Market): 시장 구분 (KOSPI, KOSDAQ).
        investor (Investor): 투자자 구분 (기관, 외국인).
        date_str (str): 날짜 문자열 (YYYYMMDD).
        raw_bytes (bytes): 다운로드된 엑셀(또는 CSV) 바이너리 데이터.
    """
    market: Market
    investor: Investor
    date_str: str
    raw_bytes: bytes
    
    @property
    def key(self) -> str:
        return f"{self.market.value}_{self.investor.value}"

@dataclass
class KrxData:
    """수집된 KRX 데이터와 메타데이터를 캡슐화하는 DTO.

    Attributes:
        market (Market): 시장 구분 (KOSPI, KOSDAQ).
        investor (Investor): 투자자 구분 (기관, 외국인).
        date_str (str): 날짜 문자열 (YYYYMMDD).
        data (pd.DataFrame): 수집된 데이터 프레임.
    """
    market: Market
    investor: Investor
    date_str: str  # YYYYMMDD
    data: pd.DataFrame

    @property
    def key(self) -> str:
        """기존 코드와의 호환성을 위한 키를 생성합니다.

        Returns:
            str: 'MARKET_investor' 형식의 키 (예: 'KOSPI_foreigner')
        """
        return f"{self.market.value}_{self.investor.value}"
