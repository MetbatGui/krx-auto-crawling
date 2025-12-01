import pytest
import pandas as pd
from core.services.daily_routine_service import DailyRoutineService
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.master_data_service import MasterDataService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.services.ranking_data_service import RankingDataService

from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from infra.adapters.watchlist_file_adapter import WatchlistFileAdapter
from infra.adapters.ranking_excel_adapter import RankingExcelAdapter
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter
from infra.adapters.excel.master_sheet_adapter import MasterSheetAdapter
from infra.adapters.excel.master_pivot_sheet_adapter import MasterPivotSheetAdapter

from tests.fakes.fake_storage_adapter import FakeStorageAdapter
from tests.fakes.fake_krx_adapter import FakeKrxAdapter

@pytest.fixture
def fake_storage():
    return FakeStorageAdapter()

@pytest.fixture
def fake_krx():
    # 테스트용 엑셀 바이너리 데이터 (실제 내용은 중요하지 않음, 파싱 에러만 안 나면 됨)
    # 하지만 pandas read_excel이 실패하지 않으려면 최소한의 헤더는 있어야 함.
    # 여기서는 간단히 빈 바이트를 넘기고, Adapter나 Service에서 예외처리가 되는지 보거나,
    # 아니면 FakeKrxAdapter가 DataFrame을 리턴하도록 수정해야 할 수도 있음.
    # KrxHttpAdapter는 bytes를 리턴하고, KrxFetchService가 이를 pd.read_excel로 읽음.
    # 따라서 bytes는 유효한 엑셀 파일이어야 함.
    
    # 간단한 엑셀 파일 생성
    df = pd.DataFrame({'종목코드': ['005930'], '종목명': ['삼성전자'], '거래대금_순매수': [1000]})
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_bytes = output.getvalue()
    
    return FakeKrxAdapter(fake_data=excel_bytes)

@pytest.fixture
def fake_storage():
    storage = FakeStorageAdapter()
    
    # RankingExcelAdapter는 기존 파일의 시트를 복사해서 사용하므로, 초기 파일이 필요함
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Template"
    storage.save_workbook(wb, "2025년/일별수급정리표/2025일별수급순위정리표.xlsx")
    
    return storage

@pytest.fixture
def daily_routine_service(fake_storage, fake_krx):
    # 1. Adapters
    save_storages = [fake_storage]
    source_storage = fake_storage
    
    daily_adapter = DailyExcelAdapter(storages=save_storages)
    watchlist_adapter = WatchlistFileAdapter(storages=save_storages)
    
    master_sheet_adapter = MasterSheetAdapter()
    master_pivot_sheet_adapter = MasterPivotSheetAdapter()
    master_workbook_adapter = MasterWorkbookAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        sheet_adapter=master_sheet_adapter,
        pivot_sheet_adapter=master_pivot_sheet_adapter
    )
    
    ranking_report_adapter = RankingExcelAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        file_name="2025년/일별수급정리표/2025일별수급순위정리표.xlsx"
    )

    # 2. Services
    fetch_service = KrxFetchService(krx_port=fake_krx)
    master_data_service = MasterDataService()
    master_service = MasterReportService(
        source_storage=source_storage, 
        target_storages=save_storages,
        data_service=master_data_service,
        workbook_adapter=master_workbook_adapter,
        file_name_prefix="2025"
    )
    
    ranking_data_service = RankingDataService(top_n=20)
    ranking_service = RankingAnalysisService(
        data_service=ranking_data_service,
        report_port=ranking_report_adapter
    )
    
    return DailyRoutineService(
        fetch_service=fetch_service,
        daily_port=daily_adapter,
        master_port=master_service,
        ranking_port=ranking_service,
        watchlist_port=watchlist_adapter
    )

@pytest.mark.asyncio
async def test_daily_routine_execution_flow(daily_routine_service, fake_storage):
    """전체 루틴이 에러 없이 실행되고, 결과 파일들이 저장소에 생성되는지 검증"""
    # Given
    target_date = "20250101"
    
    # When
    await daily_routine_service.execute(date_str=target_date)
    
    # Then
    # 1. 일별 리포트 생성 확인
    # (KOSPI/KOSDAQ * Foreigner/Institution = 4 files)
    # 파일명은 Adapter 로직에 따라 다름.
    # FakeKrx가 모든 요청에 대해 데이터를 리턴하므로 4개 다 생성되어야 함.
    
    # 저장된 파일 경로들 확인
    saved_files = list(fake_storage.dataframes.keys()) + list(fake_storage.workbooks.keys()) + list(fake_storage.files.keys())
    
    # 일별 리포트 (Excel)
    daily_reports = [f for f in saved_files if "순매수.xlsx" in f and "20250101" in f]
    assert len(daily_reports) >= 4
    
    # 2. 관심종목 파일 생성 확인 (CSV)
    watchlist_files = [f for f in saved_files if "관심종목" in f and "20250101" in f]
    assert len(watchlist_files) >= 1
    
    # 3. 마스터 리포트 생성 확인 (Workbook)
    # MasterReportService는 기존 파일이 없으면 생성함.
    master_files = [f for f in saved_files if "순매수도(2025).xlsx" in f]
    assert len(master_files) >= 4
    
    # 4. 랭킹 리포트 생성 확인
    ranking_files = [f for f in saved_files if "일별수급순위정리표.xlsx" in f]
    assert len(ranking_files) >= 1
