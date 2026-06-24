import typer
import datetime
import os
from typing import Optional, List
from dotenv import load_dotenv
from core.logger import logger

# Ports & Services
from core.services.daily_routine_service import DailyRoutineService
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.master_data_service import MasterDataService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.services.ranking_data_service import RankingDataService

# Adapters
from infra.adapters.storage import LocalStorageAdapter
from infra.adapters.native_krx_adapter import NativeKrxAdapter
from infra.adapters.naver_price_adapter import NaverPriceDataAdapter
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter
from infra.adapters.watchlist_file_adapter import WatchlistFileAdapter
from infra.adapters.ranking_excel_adapter import RankingExcelAdapter
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter
from infra.adapters.excel.master_sheet_adapter import MasterSheetAdapter
from infra.adapters.excel.master_pivot_sheet_adapter import MasterPivotSheetAdapter

def backfill(
    start: str = typer.Option(..., "--start", "-s", help="시작 날짜 (YYYYMMDD)"),
    end: str = typer.Option(None, "--end", "-e", help="종료 날짜 (YYYYMMDD, 기본값: 오늘)"),
    drive: bool = typer.Option(False, "--drive", "-d", help="Google Drive 연동 여부"),
    force: bool = typer.Option(False, "--force", "-f", help="기존 파일 존재 여부와 관계없이 강제 실행"),
    dry_run: bool = typer.Option(False, "--dry-run", help="실제 저장을 수행하지 않는 모의 백필 실행 여부")
):
    """누락된 날짜의 데이터를 자동으로 수집하고 리포트를 생성합니다.
    
    '관심종목' 폴더의 누적상위종목 파일 존재 여부를 기준으로 누락분을 판단합니다.
    """
    load_dotenv()
    
    # 1. 날짜 범위 설정
    start_date = datetime.datetime.strptime(start, "%Y%m%d").date()
    if end:
        end_date = datetime.datetime.strptime(end, "%Y%m%d").date()
    else:
        end_date = datetime.date.today()
        
    logger.info(f"[CLI:Backfill] 범위 설정: {start_date} ~ {end_date}")
    
    # 2. 저장소 초기화
    BASE_OUTPUT_PATH = "output"
    TOKEN_FILE = "secrets/token.json"
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    local_storage = LocalStorageAdapter(base_path=BASE_OUTPUT_PATH, dry_run=dry_run)
    save_storages = [local_storage]
    source_storage = local_storage

    if dry_run:
        logger.info("[CLI:Backfill] Running in Dry-run Mode (No files will be written)")

    if drive:
        root_folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")
        if os.path.exists(TOKEN_FILE):
            drive_storage = GoogleDriveAdapter(
                token_file=TOKEN_FILE,
                root_folder_id=root_folder_id,
                client_secret_file=CLIENT_SECRET_FILE if os.path.exists(CLIENT_SECRET_FILE) else None,
                dry_run=dry_run
            )
            save_storages.append(drive_storage)
            source_storage = drive_storage
            logger.info("[CLI:Backfill] Google Drive 모드 활성화 (Hybrid)")
    
    # 3. 완료된 날짜 스캔 (관심종목 폴더 활용)
    completed_dates = set()
    years_to_scan = range(start_date.year, end_date.year + 1)
    
    for year in years_to_scan:
        watchlist_dir = f"{year}년/관심종목"
        files = source_storage.list_files(watchlist_dir)
        for f in files:
            # 패턴: YYYYMMDD_누적상위종목.csv
            if "_누적상위종목.csv" in f:
                date_part = f.split("_")[0]
                if len(date_part) == 8 and date_part.isdigit():
                    completed_dates.add(date_part)
    
    logger.info(f"[CLI:Backfill] 스캔 완료: {len(completed_dates)}개 날짜 수집 확인됨")
    
    # 4. 수집 대상 날짜 추출 (평일 & 미수집)
    target_dates = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5:
            ds = curr.strftime("%Y%m%d")
            if force or (ds not in completed_dates):
                target_dates.append(ds)
        curr += datetime.timedelta(days=1)
    
    if not target_dates:
        logger.info("[CLI:Backfill] 수집할 누락 데이터가 없습니다. 종료합니다.")
        return
        
    logger.info(f"[CLI:Backfill] 총 {len(target_dates)}개 영업일 누락 발견: {target_dates}")
    
    # 5. 어댑터 및 서비스 초기화 (crawl.py 로직 준수)
    unified_krx_adapter = NativeKrxAdapter()
    watchlist_adapter = WatchlistFileAdapter(storages=save_storages)
    
    master_sheet_adapter = MasterSheetAdapter()
    master_pivot_sheet_adapter = MasterPivotSheetAdapter()
    master_workbook_adapter = MasterWorkbookAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        sheet_adapter=master_sheet_adapter,
        pivot_sheet_adapter=master_pivot_sheet_adapter
    )

    fetch_service = KrxFetchService(krx_port=unified_krx_adapter)
    master_data_service = MasterDataService()
    master_service = MasterReportService(
        source_storage=source_storage, 
        target_storages=save_storages,
        data_service=master_data_service,
        workbook_adapter=master_workbook_adapter
    )
    
    ranking_data_service = RankingDataService(top_n=30)
    naver_price_adapter = NaverPriceDataAdapter(max_workers=10)
    ranking_report_adapter = RankingExcelAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        price_port=naver_price_adapter
    )
    ranking_service = RankingAnalysisService(
        data_service=ranking_data_service,
        report_port=ranking_report_adapter
    )
    
    routine_service = DailyRoutineService(
        fetch_service=fetch_service,
        master_port=master_service,
        ranking_port=ranking_service,
        watchlist_port=watchlist_adapter
    )
    
    # 6. 실행
    success_count = 0
    fail_count = 0
    
    for ds in target_dates:
        try:
            logger.info(f"[Backfill] {ds} 처리 중...")
            routine_service.execute(date_str=ds)
            success_count += 1
        except Exception as e:
            logger.error(f"[Backfill] [Error] {ds} 처리 중 치명적 오류: {e}")
            fail_count += 1
            
    logger.info(f"[Backfill] 완료 - 성공: {success_count}, 실패: {fail_count}")
    if target_dates:
         logger.info(f"[Backfill] 대상: {target_dates}")
