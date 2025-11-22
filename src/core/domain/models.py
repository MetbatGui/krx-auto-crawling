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
    """
    수집된 KRX 데이터와 메타데이터를 캡슐화하는 DTO.
    """
    market: Market
    investor: Investor
    date_str: str  # YYYYMMDD
    data: pd.DataFrame

    @property
    def key(self) -> str:
        """
        기존 코드와의 호환성을 위한 키 생성 (e.g., 'KOSPI_foreigner')
        """
        return f"{self.market.value}_{self.investor.value}"
