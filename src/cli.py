import typer
import datetime
import os
import sys
from typing import Optional
from dotenv import load_dotenv

# Services
from core.services.daily_routine_service import DailyRoutineService
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.master_data_service import MasterDataService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.services.ranking_data_service import RankingDataService

# Adapters
from infra.adapters.storage import LocalStorageAdapter
from infra.adapters.storage.fallback_storage_adapter import FallbackStorageAdapter
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter
from infra.adapters.krx_http_adapter import KrxHttpAdapter
from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from infra.adapters.watchlist_file_adapter import WatchlistFileAdapter
from infra.adapters.ranking_excel_adapter import RankingExcelAdapter
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter
from infra.adapters.excel.master_sheet_adapter import MasterSheetAdapter
from infra.adapters.excel.master_pivot_sheet_adapter import MasterPivotSheetAdapter

app = typer.Typer(help="KRX Auto Crawling CLI")

@app.command()
def crawl(
    date: Optional[str] = typer.Argument(None, help="Target date in YYYYMMDD format (default: today)"),
    drive: bool = typer.Option(False, "--drive", "-d", help="Save to Google Drive as well")
):
    """
    Execute the daily crawling routine.
    """
    # 1. 환경 변수 로드
    load_dotenv()
    
    # 2. 날짜 처리
    if date:
        target_date = date
        # 간단한 날짜 형식 검증
        if len(target_date) != 8 or not target_date.isdigit():
            typer.echo(f"🚨 [CLI] Invalid date format: {target_date}. Please use YYYYMMDD.", err=True)
            raise typer.Exit(code=1)
    else:
        target_date = datetime.date.today().strftime('%Y%m%d')

    # 3. 기본 경로 및 설정
    BASE_OUTPUT_PATH = "output"
    SERVICE_ACCOUNT_FILE = "secrets/service-account.json"
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    
    # 4. StoragePort 인스턴스 생성
    # 모드에 따라 배타적으로 동작 (Local Only OR Drive Only)
    save_storages = []
    source_storage = None

    if drive:
        # Google Drive Mode
        root_folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")
        try:
            if os.path.exists(CLIENT_SECRET_FILE):
                print(f"[CLI] OAuth 2.0 인증 사용 ({CLIENT_SECRET_FILE})")
                drive_storage = GoogleDriveAdapter(
                    client_secret_file=CLIENT_SECRET_FILE,
                    root_folder_id=root_folder_id
                )
            elif os.path.exists(SERVICE_ACCOUNT_FILE):
                print(f"[CLI] Service Account 인증 사용 ({SERVICE_ACCOUNT_FILE})")
                drive_storage = GoogleDriveAdapter(
                    service_account_file=SERVICE_ACCOUNT_FILE,
                    root_folder_id=root_folder_id
                )
            else:
                typer.echo(f"🚨 [CLI] Google Drive 인증 파일 없음 (secrets/client_secret.json 또는 service-account.json 필요)", err=True)
                raise typer.Exit(code=1)
            
            typer.echo(f"--- [CLI] Storage Mode: Google Drive Only ---")
            save_storages = [drive_storage]
            source_storage = drive_storage

        except Exception as e:
            typer.echo(f"🚨 [CLI] Google Drive 초기화 실패: {e}", err=True)
            raise typer.Exit(code=1)
            
    else:
        # Local Mode (Default)
        typer.echo(f"--- [CLI] Storage Mode: Local Only ---")
        local_storage = LocalStorageAdapter(base_path=BASE_OUTPUT_PATH)
        save_storages = [local_storage]
        source_storage = local_storage

    # 5. 어댑터(Adapters) 인스턴스 생성 및 의존성 주입
    # (Infra Layer)
    krx_adapter = KrxHttpAdapter()
    daily_adapter = DailyExcelAdapter(storages=save_storages)
    watchlist_adapter = WatchlistFileAdapter(storages=save_storages)
    
    # Master 관련 어댑터들
    master_sheet_adapter = MasterSheetAdapter()
    master_pivot_sheet_adapter = MasterPivotSheetAdapter()
    master_workbook_adapter = MasterWorkbookAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        sheet_adapter=master_sheet_adapter,
        pivot_sheet_adapter=master_pivot_sheet_adapter
    )

    # 6. 서비스(Services) 인스턴스 생성 및 의존성 주입
    # (Core Layer)
    fetch_service = KrxFetchService(krx_port=krx_adapter)
    master_data_service = MasterDataService()
    master_service = MasterReportService(
        source_storage=source_storage, 
        target_storages=save_storages,
        data_service=master_data_service,
        workbook_adapter=master_workbook_adapter,
        file_name_prefix="2025"
    )
    
    # Ranking 서비스 조립 (헥사고날 아키텍처)
    ranking_data_service = RankingDataService(top_n=20)
    ranking_report_adapter = RankingExcelAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        file_name="2025년/일별수급정리표/2025일별수급순위정리표.xlsx"
    )
    ranking_service = RankingAnalysisService(
        data_service=ranking_data_service,
        report_port=ranking_report_adapter
    )
    
    routine_service = DailyRoutineService(
        fetch_service=fetch_service,
        daily_port=daily_adapter,
        master_port=master_service,
        ranking_port=ranking_service,
        watchlist_port=watchlist_adapter
    )

    # 7. 메인 루틴 실행
    try:
        routine_service.execute(date_str=target_date)
    except Exception as e:
        typer.echo(f"\n🚨 [CLI] Critical Error during execution: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def download(
    date: Optional[str] = typer.Argument(None, help="Target date in YYYYMMDD format (default: today)")
):
    """
    Download files from Google Drive to Local Storage.
    """
    # 1. 환경 변수 로드
    load_dotenv()
    
    # 2. 날짜 처리
    if date:
        target_date = date
        if len(target_date) != 8 or not target_date.isdigit():
            typer.echo(f"🚨 [CLI] Invalid date format: {target_date}. Please use YYYYMMDD.", err=True)
            raise typer.Exit(code=1)
    else:
        target_date = datetime.date.today().strftime('%Y%m%d')

    year = target_date[:4]
    month = target_date[4:6]

    # 3. 기본 경로 및 설정
    BASE_OUTPUT_PATH = "output"
    SERVICE_ACCOUNT_FILE = "secrets/service-account.json"
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    ROOT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")

    typer.echo(f"--- [CLI] Downloading files from Google Drive (Target: {target_date}) ---")

    # 4. 저장소 초기화
    local_storage = LocalStorageAdapter(base_path=BASE_OUTPUT_PATH)
    drive_storage = None

    try:
        if os.path.exists(CLIENT_SECRET_FILE):
            drive_storage = GoogleDriveAdapter(client_secret_file=CLIENT_SECRET_FILE, root_folder_id=ROOT_FOLDER_ID)
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            drive_storage = GoogleDriveAdapter(service_account_file=SERVICE_ACCOUNT_FILE, root_folder_id=ROOT_FOLDER_ID)
        else:
            typer.echo("🚨 [CLI] No credentials found. Cannot download from Drive.", err=True)
            raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"🚨 [CLI] Drive initialization failed: {e}", err=True)
        raise typer.Exit(code=1)

    # 5. 다운로드 대상 파일 목록 정의
    files_to_download = []

    # (1) Daily Reports
    # {Year}년/{Month}월/{Type}/{date}{name}순매수.xlsx
    investor_types = ["기관", "외국인"]
    markets = ["코스피", "코스닥"]
    
    for inv_type in investor_types:
        for market in markets:
            filename = f"{target_date}{market}{inv_type}순매수.xlsx"
            path = f"{year}년/{month}월/{inv_type}/{filename}"
            files_to_download.append(path)

    # (2) Watchlist
    # {Year}년/관심종목/{date}_일별상위종목.csv
    # {Year}년/관심종목/{date}_누적상위종목.csv
    watchlist_path_daily = f"{year}년/관심종목/{target_date}_일별상위종목.csv"
    watchlist_path_cumulative = f"{year}년/관심종목/{target_date}_누적상위종목.csv"
    files_to_download.append(watchlist_path_daily)
    files_to_download.append(watchlist_path_cumulative)

    # (3) Ranking Report
    # {Year}년/일별수급정리표/{Year}일별수급순위정리표.xlsx
    ranking_path = f"{year}년/일별수급정리표/{year}일별수급순위정리표.xlsx"
    files_to_download.append(ranking_path)

    # (4) Master Reports
    # {Year}년/{name}({Year}).xlsx
    master_files = [
        f"코스피외국인순매수도({year}).xlsx",
        f"코스닥외국인순매수도({year}).xlsx",
        f"코스피기관순매수도({year}).xlsx",
        f"코스닥기관순매수도({year}).xlsx"
    ]
    for mf in master_files:
        files_to_download.append(f"{year}년/{mf}")

    # 6. 다운로드 실행
    success_count = 0
    fail_count = 0

    for file_path in files_to_download:
        typer.echo(f"Downloading: {file_path} ... ", nl=False)
        
        # Drive에서 읽기
        data = drive_storage.get_file(file_path)
        if data:
            # Local에 쓰기
            if local_storage.put_file(file_path, data):
                typer.echo("✅ OK")
                success_count += 1
            else:
                typer.echo("❌ Write Failed")
                fail_count += 1
        else:
            typer.echo("⚠️ Not Found on Drive")
            fail_count += 1

    typer.echo(f"--- [CLI] Download Complete. Success: {success_count}, Failed/Missing: {fail_count} ---")

@app.command()
def auth():
    """
    Authenticate with Google Drive (OAuth 2.0) and generate token.json.
    """
    load_dotenv()
    
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    
    if not os.path.exists(CLIENT_SECRET_FILE):
        typer.echo(f"🚨 [CLI] Client Secret file not found: {CLIENT_SECRET_FILE}", err=True)
        typer.echo("Please place your client_secret.json in the secrets directory.", err=True)
        raise typer.Exit(code=1)
        
    try:
        typer.echo("--- [CLI] Starting Google Drive Authentication ---")
        # GoogleDriveAdapter 초기화 시 인증 로직이 수행됨
        # root_folder_id는 인증 과정에 필요 없으므로 None으로 전달하거나 더미 값 사용
        adapter = GoogleDriveAdapter(client_secret_file=CLIENT_SECRET_FILE)
        
        # 인증 성공 확인 (service 객체가 생성되었는지)
        if adapter.drive_service:
            typer.echo("✅ [CLI] Authentication successful! 'secrets/token.json' has been created/updated.")
        else:
            typer.echo("❌ [CLI] Authentication failed.", err=True)
            
    except Exception as e:
        typer.echo(f"🚨 [CLI] Authentication error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def healthcheck():
    """
    Verify Google Drive access and root folder existence.
    """
    load_dotenv()
    
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    SERVICE_ACCOUNT_FILE = "secrets/service-account.json"
    ROOT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    
    typer.echo("--- [CLI] Starting Health Check ---")
    
    # 1. Credential File Check
    if os.path.exists(CLIENT_SECRET_FILE):
        typer.echo(f"✅ Credential File Found: {CLIENT_SECRET_FILE} (OAuth)")
        adapter = GoogleDriveAdapter(client_secret_file=CLIENT_SECRET_FILE, root_folder_id=ROOT_FOLDER_ID)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        typer.echo(f"✅ Credential File Found: {SERVICE_ACCOUNT_FILE} (Service Account)")
        adapter = GoogleDriveAdapter(service_account_file=SERVICE_ACCOUNT_FILE, root_folder_id=ROOT_FOLDER_ID)
    else:
        typer.echo("❌ Credential File Not Found!")
        raise typer.Exit(code=1)
        
    # 2. Drive Access & Root Folder Check
    try:
        # GoogleDriveAdapter 초기화 시 _authenticate()와 _get_or_create_folder()가 호출됨
        # root_folder_id가 있으면 해당 ID의 폴더가 존재하는지 확인하는 로직이 내장되어 있지는 않지만,
        # API 호출을 통해 검증 가능
        
        typer.echo(f"ℹ️  Checking Root Folder ID: {adapter.root_folder_id}")
        
        # 간단한 파일 목록 조회로 접근 권한 확인
        query = f"'{adapter.root_folder_id}' in parents and trashed = false"
        results = adapter.drive_service.files().list(q=query, pageSize=5, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        typer.echo("✅ Google Drive Access: OK")
        typer.echo(f"✅ Root Folder Access: OK (Found {len(files)} files/folders in root)")
        
        if files:
            typer.echo("   [Recent Files]")
            for f in files:
                typer.echo(f"   - {f['name']} ({f['id']})")
        else:
            typer.echo("   (Root folder is empty)")
            
    except Exception as e:
        typer.echo(f"❌ Google Drive Access Failed: {e}", err=True)
        raise typer.Exit(code=1)
        
    typer.echo("--- [CLI] Health Check Passed ---")

if __name__ == "__main__":
    app()
