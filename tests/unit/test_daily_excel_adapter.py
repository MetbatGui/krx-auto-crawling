import pytest
import pandas as pd
from core.domain.models import KrxData, Market, Investor
from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from tests.fakes.fake_storage_adapter import FakeStorageAdapter

def test_save_daily_reports_successfully_saves_to_storage():
    """DailyExcelAdapter가 데이터를 올바른 경로와 포맷으로 저장하는지 검증"""
    # Given
    fake_storage = FakeStorageAdapter()
    adapter = DailyExcelAdapter(storages=[fake_storage])
    
    # 테스트용 데이터 생성
    df = pd.DataFrame({
        '종목명': ['삼성전자', 'SK하이닉스'],
        '거래대금_순매수': [1000, 2000]
    })
    
    data = KrxData(
        market=Market.KOSPI, 
        investor=Investor.FOREIGNER, 
        data=df, 
        date_str='20231001'
    )

    # When
    adapter.save_daily_reports([data])

    # Then
    expected_path = "2023년/10월/외국인/20231001코스피외국인순매수.xlsx"
    
    # 1. 파일이 저장되었는지 확인 (FakeStorage 상태 검증)
    assert expected_path in fake_storage.workbooks
    
    # 2. 저장된 데이터 내용 검증
    wb = fake_storage.workbooks[expected_path]
    ws = wb.active
    rows = list(ws.values)
    
    # 헤더 포함 3행 (헤더 + 데이터 2행)
    assert len(rows) == 3
    assert rows[1][0] == '삼성전자'  # 첫 번째 데이터 행의 첫 번째 열
    
    # 3. 포맷팅 로직 검증 (1000 -> "1,000")
    # 엑셀에서는 숫자가 문자열로 저장될 수 있으므로 확인
    assert str(rows[1][1]) == "1,000"
    assert str(rows[2][1]) == "2,000"

def test_save_daily_reports_skips_empty_data():
    """빈 데이터는 저장하지 않아야 함"""
    # Given
    fake_storage = FakeStorageAdapter()
    adapter = DailyExcelAdapter(storages=[fake_storage])
    
    empty_df = pd.DataFrame()
    data = KrxData(
        market=Market.KOSPI, 
        investor=Investor.FOREIGNER, 
        data=empty_df, 
        date_str='20231001'
    )

    # When
    adapter.save_daily_reports([data])

    # Then
    assert len(fake_storage.workbooks) == 0
