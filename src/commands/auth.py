import typer
import os
from dotenv import load_dotenv
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

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
