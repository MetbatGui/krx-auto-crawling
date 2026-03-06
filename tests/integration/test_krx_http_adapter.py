import pytest
import datetime
from core.domain.models import Market, Investor
from infra.adapters.krx_http_adapter import KrxHttpAdapter

@pytest.fixture
def adapter():
    return KrxHttpAdapter()

@pytest.mark.integration
def test_krx_login_success(adapter):
    """실제 KRX 서버에 로그인하여 세션을 획득하는지 검증합니다."""
    # 처음에는 로그아웃 상태여야 함
    assert adapter.is_logged_in is False
    
    # 로그인 시도
    adapter._login()
    
    # 로그인 성공 상태로 변경되어야 함
    assert adapter.is_logged_in is True
    # 쿠키에 JSESSIONID 등이 세팅되었는지 간접 확인 (기본 쿠키 세팅 확인)
    assert 'mdc.client_session' in adapter.session.cookies.get_dict(domain='data.krx.co.kr')

@pytest.mark.integration
def test_fetch_net_value_data_success(adapter):
    """실제로 KOSPI 기관 순매수 데이터를 가져오는지 검증합니다."""
    # 주말에는 데이터가 없을 수 있으므로 최근 평일 날짜를 하드코딩하거나, 
    # 어댑터가 내부적으로 파싱하는 로직을 믿고 가장 최근 거래일로 시도해볼 수 있습니다.
    # 여기서는 데이터가 확실히 있는 과거 특정 평일 날짜를 사용해 안정적으로 테스트합니다.
    target_date = "20250228"
    
    data = adapter.fetch_net_value_data(
        market=Market.KOSPI,
        investor=Investor.INSTITUTIONS,
        date_str=target_date
    )
    
    # 1. 반환값이 bytes 형인지 확인
    assert isinstance(data, bytes)
    # 2. 데이터가 비어있지 않은지 (0바이트 이상) 확인
    assert len(data) > 0
    # 3. 로그인 상태가 유지되는지 확인 (fetch_net_value_data 내부에서 _login 호출)
    assert adapter.is_logged_in is True
