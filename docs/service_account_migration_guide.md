# Google Drive 인증 마이그레이션 가이드 (OAuth 2.0 → Service Account)

이 문서는 기존 로컬 사용자 인증(OAuth 2.0) 방식에서 **서버 간 인증(Service Account)** 방식으로 Google Drive API 인증을 전환하는 과정을 설명합니다.

이 방식은 브라우저를 통한 사용자 개입이 필요 없으므로, **CI/CD 파이프라인(GitHub Actions 등) 및 백그라운드 작업**에 훨씬 적합합니다.

---

## 1. 사전 준비 (Google Cloud Console)

### 1.1 Service Account 생성
1. [Google Cloud Console](https://console.cloud.google.com/)에 접속하여 프로젝트를 선택합니다.
2. **IAM 및 관리자** > **서비스 계정**으로 이동합니다.
3. **+ 서비스 계정 만들기**를 클릭합니다.
   - 이름: 예) `drive-api-bot`
   - 역할: 필요 시 부여 (선택 사항, Drive API 접근 자체에는 필수 아님)
4. 생성된 서비스 계정을 클릭하고 **키** 탭으로 이동합니다.
5. **키 추가** > **새 키 만들기** > **JSON**을 선택하여 키 파일을 다운로드합니다.
   - 파일명 예시: `service_account.json`

### 1.2 Google Drive 폴더 공유 (중요!)
Service Account는 독립적인 이메일 주소(예: `drive-api-bot@project-id.iam.gserviceaccount.com`)를 가집니다.
따라서 **기존 내 드라이브의 폴더에 접근하려면 해당 폴더를 공유**해야 합니다.

1. 접근할 Google Drive 폴더(예: `KRX_Auto_Crawling_Data`)를 우클릭 > **공유**.
2. Service Account의 **이메일 주소**를 입력하고 **편집자(Editor)** 권한을 부여합니다.
   - *이 단계가 없으면 Service Account는 해당 폴더를 찾을 수 없습니다.*

---

## 2. 코드 변경 사항 (Python)

기존 `google-auth-oauthlib`(InstalledAppFlow) 의존성을 제거하고, `google-oauth2`(service_account)를 사용하도록 변경합니다.

### 2.1 의존성 변경

**제거 (더 이상 불필요):**
```python
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
```

**추가/유지:**
```python
from google.oauth2 import service_account
from googleapiclient.discovery import build
```

### 2.2 인증 로직 변경 (`GoogleDriveAdapter`)

**변경 전 (OAuth 2.0):**
```python
# token.json 로드 또는 브라우저 팝업을 통한 키 발급
if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
if not creds:
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
```

**변경 후 (Service Account):**
```python
# 서비스 계정 키 파일로 직접 인증 (브라우저 불필요)
creds = service_account.Credentials.from_service_account_file(
    service_account_file_path, 
    scopes=SCOPES
)
service = build('drive', 'v3', credentials=creds)
```

---

## 3. 비밀 변수 및 CI/CD 설정 (GitHub Actions 예시)

`create-json` 액션 등을 사용하여 CI 환경에 JSON 키 파일을 주입해야 합니다.

### 3.1 GitHub Secrets 등록
1. GitHub 저장소 > **Settings** > **Secrets and variables** > **Actions**.
2. **New repository secret** 클릭.
3. `GOOGLE_SERVICE_ACCOUNT_JSON`: 다운로드 받은 `service_account.json` 파일의 **전체 내용**을 복사하여 붙여넣기.

### 3.2 Workflow YAML 수정
기존 `client_secret.json` 및 `token.json` 생성 단계를 `service_account.json` 생성으로 대체합니다.

```yaml
jobs:
  build:
    steps:
    - name: Create Secrets
      run: |
        mkdir -p secrets
        # 기존 OAuth 관련 시크릿 제거
        # echo '${{ secrets.GOOGLE_CLIENT_SECRET_JSON }}' > secrets/client_secret.json
        # echo '${{ secrets.GOOGLE_TOKEN_JSON }}' > secrets/token.json
        
        # [NEW] Service Account 키 파일 생성
        echo '${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}' > secrets/service_account.json
```

---

## 4. 로컬 개발 환경 설정

로컬에서도 동일하게 `secrets/service_account.json` 파일을 위치시키면, 별도의 브라우저 로그인 과정 없이 바로 작업이 가능합니다.

1. 프로젝트 루트의 `secrets/` 폴더에 `service_account.json` 파일 복사.
2. `.gitignore`에 반드시 `secrets/*.json`이 포함되어 있는지 재확인 (보안 사고 방지).

---

## 5. 요약: 마이그레이션 체크리스트

- [ ] Google Cloud Console에서 Service Account 생성 및 JSON 키 다운로드.
- [ ] Google Drive 대상 폴더에 Service Account 이메일 초대 (편집자 권한).
- [ ] 프로젝트 코드에서 OAuth 관련 로직 제거 및 Service Account 로직 구현.
- [ ] GitHub Actions Secrets에 키 내용 등록 (`GOOGLE_SERVICE_ACCOUNT_JSON`).
- [ ] CI/CD 워크플로우 YAML 파일 업데이트.
