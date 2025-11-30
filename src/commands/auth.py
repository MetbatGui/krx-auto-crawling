import typer
import os
from dotenv import load_dotenv
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def auth():
    """
    Google Drive 인증(OAuth 2.0)을 수행하고 token.json을 생성합니다.
    """
    load_dotenv()
    
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    
    if not os.path.exists(CLIENT_SECRET_FILE):
        typer.echo(f"🚨 [CLI] Client Secret 파일을 찾을 수 없습니다: {CLIENT_SECRET_FILE}", err=True)
        typer.echo("secrets 디렉토리에 client_secret.json 파일을 위치시켜주세요.", err=True)
        raise typer.Exit(code=1)
        
    try:
        typer.echo("--- [CLI] Google Drive 인증 시작 ---")
        # GoogleDriveAdapter 초기화 시 인증 로직이 수행됨
        # root_folder_id는 인증 과정에 필요 없으므로 None으로 전달하거나 더미 값 사용
        adapter = GoogleDriveAdapter(client_secret_file=CLIENT_SECRET_FILE)
        
        # 인증 성공 확인 (service 객체가 생성되었는지)
        if adapter.drive_service:
            typer.echo("✅ [CLI] 인증 성공! 'secrets/token.json' 파일이 생성/갱신되었습니다.")
        else:
            typer.echo("❌ [CLI] 인증 실패.", err=True)
            
    except Exception as e:
        typer.echo(f"🚨 [CLI] 인증 오류: {e}", err=True)
        raise typer.Exit(code=1)
