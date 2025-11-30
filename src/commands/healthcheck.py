import typer
import os
from dotenv import load_dotenv
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

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
