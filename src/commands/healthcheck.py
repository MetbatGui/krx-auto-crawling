import typer
import os
from dotenv import load_dotenv
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def healthcheck():
    """
    Google Drive 접근 권한 및 루트 폴더 존재 여부를 확인합니다.
    """
    load_dotenv()
    
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    SERVICE_ACCOUNT_FILE = "secrets/service-account.json"
    ROOT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    
    typer.echo("--- [CLI] 헬스 체크 시작 ---")
    
    # 1. Credential File Check
    if os.path.exists(CLIENT_SECRET_FILE):
        typer.echo(f"✅ 인증 파일 확인됨: {CLIENT_SECRET_FILE} (OAuth)")
        adapter = GoogleDriveAdapter(client_secret_file=CLIENT_SECRET_FILE, root_folder_id=ROOT_FOLDER_ID)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        typer.echo(f"✅ 인증 파일 확인됨: {SERVICE_ACCOUNT_FILE} (Service Account)")
        adapter = GoogleDriveAdapter(service_account_file=SERVICE_ACCOUNT_FILE, root_folder_id=ROOT_FOLDER_ID)
    else:
        typer.echo("❌ 인증 파일을 찾을 수 없습니다!")
        raise typer.Exit(code=1)
        
    # 2. Drive Access & Root Folder Check
    try:
        # GoogleDriveAdapter 초기화 시 _authenticate()와 _get_or_create_folder()가 호출됨
        # root_folder_id가 있으면 해당 ID의 폴더가 존재하는지 확인하는 로직이 내장되어 있지는 않지만,
        # API 호출을 통해 검증 가능
        
        typer.echo(f"ℹ️  루트 폴더 ID 확인: {adapter.root_folder_id}")
        
        # 간단한 파일 목록 조회로 접근 권한 확인
        query = f"'{adapter.root_folder_id}' in parents and trashed = false"
        results = adapter.drive_service.files().list(q=query, pageSize=5, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        typer.echo("✅ Google Drive 접근: 성공")
        typer.echo(f"✅ 루트 폴더 접근: 성공 (루트 내 {len(files)}개 파일/폴더 발견)")
        
        if files:
            typer.echo("   [최근 파일]")
            for f in files:
                typer.echo(f"   - {f['name']} ({f['id']})")
        else:
            typer.echo("   (루트 폴더가 비어있습니다)")
            
    except Exception as e:
        typer.echo(f"❌ Google Drive 접근 실패: {e}", err=True)
        raise typer.Exit(code=1)
        
    typer.echo("--- [CLI] 헬스 체크 통과 ---")
