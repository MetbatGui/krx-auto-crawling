from typing import Optional
from core.ports.krx_data_port import KrxDataPort
from core.domain.models import Market, Investor

class FakeKrxAdapter(KrxDataPort):
    """테스트용 Fake KRX 어댑터.
    
    실제 네트워크 요청 없이 미리 주입된 데이터를 반환합니다.
    """
    def __init__(self, fake_data: bytes = b"FAKE_EXCEL_BINARY"):
        self.fake_data = fake_data
        self.call_history = []

    def fetch_net_value_data(self, market: Market, investor: Investor, date_str: Optional[str] = None) -> bytes:
        self.call_history.append({
            'market': market,
            'investor': investor,
            'date_str': date_str
        })
        return self.fake_data
