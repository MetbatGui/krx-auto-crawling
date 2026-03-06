import pytest
from core.domain.models import Market, Investor
from infra.adapters.native_krx_adapter import NativeKrxAdapter

from dotenv import load_dotenv

@pytest.fixture(autouse=True)
def load_env():
    load_dotenv()

@pytest.fixture
def adapter():
    return NativeKrxAdapter()

@pytest.mark.integration
def test_krx_login_success(adapter):
    """최초 인스턴스 생성 시 is_logged_in은 False지만 _login() 호출 후 True로 변경됨을 검증"""
    assert adapter.is_logged_in is False
    adapter._login()
    assert adapter.is_logged_in is True
    assert 'mdc.client_session' in adapter.session.cookies
    assert 'JSESSIONID' in adapter.session.cookies

@pytest.mark.integration
def test_fetch_net_value_data_success(adapter):
    """통합 어댑터로 순매수 엑셀 데이터를 정상적으로 가져오는지 검증"""
    target_date = "20250228"
    
    file_bytes = adapter.fetch_net_value_data(
        market=Market.KOSPI, 
        investor=Investor.FOREIGNER, 
        date_str=target_date
    )
    
    assert file_bytes is not None
    assert type(file_bytes) is bytes
    assert len(file_bytes) > 1000  # 최소 1KB 이상 정상 데이터 응답 확인

@pytest.mark.integration
def test_get_price_info_success(adapter):
    """삼성전자(005930)의 특정 과거 날짜 기준 가격 정보 생성을 검증"""
    # 2025-02-28 (금요일) 삼성전자 종가
    target_date = "20250228"
    ticker = "005930"
    
    price_info = adapter.get_price_info(ticker, target_date)
    
    # 1. 반환값이 정상적으로 StockPriceInfo 인지 확인
    assert price_info is not None
    assert price_info.ticker == ticker
    
    # 2. 데이터 유효성 검증
    assert price_info.close_price > 0
    assert price_info.all_time_high >= price_info.high_52w
    assert price_info.high_52w > 0

@pytest.mark.integration
def test_get_price_info_invalid_ticker(adapter):
    """존재하지 않는 종목코드에 대한 처리 검증"""
    price_info = adapter.get_price_info("999999", "20250228")
    assert price_info is None
