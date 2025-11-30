import typer
import datetime
import os
from dotenv import load_dotenv

from infra.adapters.storage import LocalStorageAdapter
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def download(
    date: str = typer.Argument(None, help="Target date in YYYYMMDD format (default: today)")
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
