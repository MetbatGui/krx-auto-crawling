import pytest
import pandas as pd
from core.services.krx_fetch_service import KrxFetchService
from core.domain.models import Market, Investor
from tests.fakes.fake_krx_adapter import FakeKrxAdapter

@pytest.mark.asyncio
async def test_fetch_all_data_success():
    """전체 데이터 수집 성공 케이스 검증"""
    # Given
    # 유효한 엑셀 바이너리 생성 (순매수_거래대금 컬럼 포함)
    df = pd.DataFrame({'종목코드': ['005930'], '종목명': ['삼성전자'], '순매수_거래대금': [1000]})
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_bytes = output.getvalue()
    
    fake_adapter = FakeKrxAdapter(fake_data=excel_bytes)
    service = KrxFetchService(krx_port=fake_adapter)
    
    # When
    results = await service.fetch_all_data("20250101")
    
    # Then
    # 4개 타겟(KOSPI/KOSDAQ * Foreigner/Institutions)에 대해 모두 성공해야 함
    assert len(results) == 4
    assert results[0].date_str == "20250101"
    assert not results[0].data.empty
    assert results[0].data['종목명'].iloc[0] == '삼성전자'

def test_parse_and_filter_data_handles_empty_bytes():
    """빈 바이트 데이터 처리 검증"""
    # Given
    service = KrxFetchService(krx_port=FakeKrxAdapter())
    
    # When
    df = service._parse_and_filter_data(b"")
    
    # Then
    assert df.empty

def test_parse_and_filter_data_handles_parsing_error():
    """파싱 에러 처리 검증 (유효하지 않은 엑셀 파일)"""
    # Given
    service = KrxFetchService(krx_port=FakeKrxAdapter())
    
    # When
    df = service._parse_and_filter_data(b"INVALID_EXCEL_DATA")
    
    # Then
    assert df.empty

def test_parse_and_filter_data_extracts_top_20():
    """상위 20개 추출 및 컬럼명 변경 검증"""
    # Given
    # 30개 데이터 생성
    data = {
        '종목코드': [f'{i}' for i in range(30)],
        '종목명': [f'Stock{i}' for i in range(30)],
        '순매수_거래대금': [i * 100 for i in range(30)] # 0, 100, ..., 2900
    }
    df = pd.DataFrame(data)
    
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_bytes = output.getvalue()
    
    service = KrxFetchService(krx_port=FakeKrxAdapter())
    
    # When
    result_df = service._parse_and_filter_data(excel_bytes)
    
    # Then
    assert len(result_df) == 20
    # 가장 큰 값이 첫 번째여야 함 (2900)
    assert result_df['순매수_거래대금'].iloc[0] == 2900
    assert result_df['종목명'].iloc[0] == 'Stock29'
