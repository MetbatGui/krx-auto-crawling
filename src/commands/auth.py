import typer
import os
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def auth():
    """Google Drive OAuth 2.0 인증을 수행합니다.

    `secrets/client_secret.json` 파일을 사용하여 사용자 인증을 진행하고,
    `secrets/token.json` 파일을 생성합니다.
    """
    load_dotenv()
    
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    TOKEN_FILE = "secrets/token.json"
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    if not os.path.exists(CLIENT_SECRET_FILE):
        typer.echo(f"🚨 [CLI] Client Secret 파일을 찾을 수 없습니다: {CLIENT_SECRET_FILE}", err=True)
        typer.echo("Google Cloud Console에서 OAuth Client ID를 생성하고 secrets 디렉토리에 client_secret.json 파일로 저장해주세요.", err=True)
        raise typer.Exit(code=1)
        
    try:
        typer.echo("--- [CLI] Google Drive OAuth 2.0 인증 시작 ---")
        
        flow = InstalledAppFlow.from_client_secrets_file(
            CLIENT_SECRET_FILE, SCOPES
        )
        creds = flow.run_local_server(port=0)
        
        # 토큰 저장
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
            
        typer.echo(f"✅ [CLI] 인증 성공! 토큰이 저장되었습니다: {TOKEN_FILE}")
        
        # 검증
        adapter = GoogleDriveAdapter(token_file=TOKEN_FILE)
        if adapter.drive_service:
             typer.echo("✅ [CLI] Google Drive 연결 테스트 완료")

    except Exception as e:
        typer.echo(f"🚨 [CLI] 인증 오류: {e}", err=True)
        raise typer.Exit(code=1)
