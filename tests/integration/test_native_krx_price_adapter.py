import pytest
from core.domain.models import Market, Investor
from infra.adapters.native_krx_price_adapter import NativeKrxPriceAdapter

from dotenv import load_dotenv

@pytest.fixture(autouse=True)
def load_env():
    load_dotenv()

@pytest.fixture
def adapter():
    return NativeKrxPriceAdapter()

@pytest.mark.integration
def test_get_price_info_success(adapter):
    """삼성전자(005930)의 특정 과거 날짜 기준 가격 정보 조회를 검증합니다."""
    # 2025-02-28 (금요일) 삼성전자 종가
    target_date = "20250228"
    ticker = "005930"
    
    price_info = adapter.get_price_info(ticker, target_date)
    
    # 1. 반환값이 정상적으로 StockPriceInfo 인지 확인
    assert price_info is not None
    assert price_info.ticker == ticker
    
    # 2. 데이터 유효성 검증
    # 2025년 2월 28일 기준 삼성전자:
    # 종가는 0보다 커야 함
    assert price_info.close_price > 0
    # 역사적 신고가는 종가/52주 신고가보다 크거나 같아야 함
    assert price_info.all_time_high >= price_info.high_52w
    assert price_info.high_52w > 0
    
    # 신고가 로직 기반 Boolean 값 테스트 (문법 오류 안 나는지)
    _ = price_info.is_52w_high
    _ = price_info.is_all_time_high
    _ = price_info.is_near_52w_high
    _ = price_info.is_near_all_time_high
    
@pytest.mark.integration
def test_get_price_info_invalid_ticker(adapter):
    """존재하지 않는 종목코드에 대한 처리 검증."""
    price_info = adapter.get_price_info("999999", "20250228")
    assert price_info is None
