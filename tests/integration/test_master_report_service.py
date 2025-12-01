import pytest
import pandas as pd
from core.services.master_report_service import MasterReportService
from core.services.master_data_service import MasterDataService
from core.domain.models import KrxData, Market, Investor
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter
from infra.adapters.excel.master_sheet_adapter import MasterSheetAdapter
from infra.adapters.excel.master_pivot_sheet_adapter import MasterPivotSheetAdapter
from tests.fakes.fake_storage_adapter import FakeStorageAdapter

@pytest.fixture
def fake_storage():
    return FakeStorageAdapter()

@pytest.fixture
def master_service(fake_storage):
    # 어댑터 조립
    sheet_adapter = MasterSheetAdapter()
    pivot_adapter = MasterPivotSheetAdapter()
    workbook_adapter = MasterWorkbookAdapter(
        source_storage=fake_storage,
        target_storages=[fake_storage],
        sheet_adapter=sheet_adapter,
        pivot_sheet_adapter=pivot_adapter
    )
    
    data_service = MasterDataService()
    
    return MasterReportService(
        source_storage=fake_storage,
        target_storages=[fake_storage],
        data_service=data_service,
        workbook_adapter=workbook_adapter,
        file_name_prefix="2025"
    )

@pytest.mark.asyncio
async def test_master_report_update_creates_new_file_if_not_exists(master_service, fake_storage):
    """파일이 없을 때 새로 생성하는지 검증"""
    # Given
    df = pd.DataFrame({'종목코드': ['005930'], '종목명': ['삼성전자'], '순매수_거래대금': [1000]})
    data = KrxData(Market.KOSPI, Investor.FOREIGNER, "20250101", df)
    
    # When
    await master_service.update_reports([data])
    
    # Then
    expected_filename = "2025년/코스피외국인순매수도(2025).xlsx"
    assert expected_filename in fake_storage.workbooks
    
    # 시트 생성 확인 (JAN 시트)
    wb = fake_storage.workbooks[expected_filename]
    assert "JAN" in wb.sheetnames
    assert "0101" in wb.sheetnames # 피벗 시트

@pytest.mark.asyncio
async def test_master_report_update_appends_to_existing_file(master_service, fake_storage):
    """이미 파일이 있을 때 데이터를 추가하는지 검증"""
    # Given
    # 1. 먼저 파일 생성 (1월 1일 데이터)
    df1 = pd.DataFrame({'종목코드': ['005930'], '종목명': ['삼성전자'], '순매수_거래대금': [1000]})
    data1 = KrxData(Market.KOSPI, Investor.FOREIGNER, "20250101", df1)
    await master_service.update_reports([data1])
    
    # 2. 새로운 데이터 추가 (1월 2일 데이터)
    df2 = pd.DataFrame({'종목코드': ['000660'], '종목명': ['SK하이닉스'], '순매수_거래대금': [2000]})
    data2 = KrxData(Market.KOSPI, Investor.FOREIGNER, "20250102", df2)
    
    # When
    await master_service.update_reports([data2])
    
    # Then
    expected_filename = "2025년/코스피외국인순매수도(2025).xlsx"
    wb = fake_storage.workbooks[expected_filename]
    
    # 시트 확인
    assert "JAN" in wb.sheetnames
    assert "0102" in wb.sheetnames
    
    # JAN 시트에 데이터가 누적되었는지 확인 (헤더 포함 최소 3행 이상이어야 함)
    ws = wb["JAN"]
    assert ws.max_row >= 3 

@pytest.mark.asyncio
async def test_master_report_skips_empty_data(master_service, fake_storage):
    """빈 데이터는 처리를 건너뛰는지 검증"""
    # Given
    empty_df = pd.DataFrame()
    data = KrxData(Market.KOSPI, Investor.FOREIGNER, "20250101", empty_df)
    
    # When
    await master_service.update_reports([data])
    
    # Then
    # 파일이 생성되지 않아야 함
    assert len(fake_storage.workbooks) == 0
