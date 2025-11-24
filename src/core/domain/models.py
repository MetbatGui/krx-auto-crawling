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
class KrxData:
    """수집된 KRX 데이터와 메타데이터를 캡슐화하는 DTO.

    Attributes:
        market (Market): 시장 구분
        investor (Investor): 투자자 구분
        date_str (str): 날짜 (YYYYMMDD)
        data (pd.DataFrame): 수집된 데이터
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
