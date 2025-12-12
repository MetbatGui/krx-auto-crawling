import typer
import os
from dotenv import load_dotenv
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def auth():
    """Service Account 인증 상태를 검증합니다.

    `secrets/service_account.json` 파일을 사용하여 Google Drive 접근 권한을 확인합니다.
    """
    load_dotenv()
    
    SERVICE_ACCOUNT_FILE = "secrets/service_account.json"
    
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        typer.echo(f"🚨 [CLI] Service Account 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}", err=True)
        typer.echo("secrets 디렉토리에 service_account.json 파일을 위치시켜주세요.", err=True)
        raise typer.Exit(code=1)
        
    try:
        typer.echo("--- [CLI] Service Account 인증 검증 시작 ---")
        # GoogleDriveAdapter 초기화 시 인증 로직이 수행됨
        adapter = GoogleDriveAdapter(service_account_file=SERVICE_ACCOUNT_FILE)
        
        # 인증 성공 확인 (service 객체가 생성되었는지)
        if adapter.drive_service:
            typer.echo("✅ [CLI] Service Account 인증 성공!")
            # 간단한 API 호출 테스트
            adapter.drive_service.files().list(pageSize=1).execute()
        else:
            typer.echo("❌ [CLI] 인증 실패.", err=True)
            
    except Exception as e:
        typer.echo(f"🚨 [CLI] 인증 오류: {e}", err=True)
        raise typer.Exit(code=1)
