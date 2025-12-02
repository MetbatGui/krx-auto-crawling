# KRX 자동 크롤링 및 리포트 생성 시스템 (KRX Auto Crawling)

KRX(한국거래소)의 일별 거래 데이터를 자동으로 수집하고, 기관 및 외국인의 순매수 동향을 분석하여 엑셀 리포트로 생성하는 자동화 시스템입니다. 생성된 리포트는 로컬 저장소 및 구글 드라이브에 자동으로 동기화됩니다.

## ✨ 주요 기능

*   **자동 데이터 수집**: 매일 KRX에서 코스피/코스닥 시장의 기관/외국인 순매수 데이터를 수집합니다.
*   **리포트 생성**: 수집된 데이터를 바탕으로 가독성 높은 엑셀 리포트(일별, 누적, 순위)를 생성합니다.
*   **수급 분석**: 특정 기간 동안의 수급 주체별 순매수 상위 종목을 분석하고 랭킹을 매깁니다.
*   **클라우드 동기화**: 생성된 모든 데이터와 리포트를 Google Drive에 자동으로 업로드하여 어디서든 접근 가능하게 합니다.
*   **안정적인 실행**: Docker 컨테이너 기반으로 실행되어 환경에 구애받지 않는 일관된 동작을 보장합니다.
*   **알림 및 모니터링**: GitHub Actions를 통해 평일 자동 실행되며, 실행 결과를 모니터링할 수 있습니다.

## 🛠 기술 스택

*   **Language**: Python 3.14
*   **Package Manager**: `uv` (Fast Python package installer and resolver)
*   **Core Libraries**:
    *   `pandas`: 데이터 처리 및 분석
    *   `playwright`: 웹 크롤링 및 자동화
    *   `openpyxl`, `xlsxwriter`: 엑셀 파일 생성 및 스타일링
    *   `google-api-python-client`: Google Drive API 연동
*   **Architecture**: Hexagonal Architecture (Ports and Adapters)
*   **Infrastructure**: Docker, GitHub Actions
*   **Task Runner**: `just`

## 🚀 시작하기 (Getting Started)

### 사전 요구 사항 (Prerequisites)

*   [Docker](https://www.docker.com/) 및 Docker Compose
*   [Just](https://github.com/casey/just) (선택 사항, 명령어 실행 편의용)
*   Google Cloud Platform 프로젝트 및 서비스 계정/OAuth 자격 증명

### 설치 및 설정 (Setup)

1.  **레포지토리 클론**
    ```bash
    git clone https://github.com/MetbatGui/krx-auto-crawling.git
    cd krx-auto-crawling
    ```

2.  **환경 변수 설정**
    `.env` 파일을 생성하고 필요한 환경 변수를 설정합니다.
    ```bash
    # .env 예시
    GOOGLE_DRIVE_ROOT_FOLDER_ID=your_folder_id_here
    KRX_OTP_URL=http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd
    KRX_DOWNLOAD_URL=http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd
    ```

3.  **시크릿 파일 설정**
    `secrets/` 디렉토리를 생성하고 Google 인증 파일을 위치시킵니다.
    *   `secrets/service-account.json`: 서비스 계정 키 (서버용)
    *   `secrets/client_secret.json`: OAuth 클라이언트 시크릿 (로컬용)

### 실행 방법 (Usage)

`just` 명령어를 사용하여 간편하게 실행할 수 있습니다.

*   **Docker 이미지 빌드**
    ```bash
    just build
    ```

*   **일일 크롤링 실행 (오늘 날짜)**
    ```bash
    just crawl
    ```

*   **특정 날짜 크롤링 및 구글 드라이브 업로드**
    ```bash
    just crawl 20251201 --drive
    ```

*   **구글 드라이브에서 데이터 다운로드**
    ```bash
    just download 20251201
    ```

*   **헬스 체크 (연동 확인)**
    ```bash
    just docker-healthcheck
    ```

## 📂 프로젝트 구조

```
src/
├── core/               # 비즈니스 로직 (Domain, Services, Ports)
│   ├── domain/         # 도메인 모델 (KrxData 등)
│   ├── ports/          # 인터페이스 정의 (StoragePort, KrxDataPort 등)
│   └── services/       # 애플리케이션 서비스 (DailyRoutineService 등)
├── infra/              # 외부 어댑터 (Adapters)
│   ├── adapters/       # 구현체 (GoogleDriveAdapter, LocalStorageAdapter 등)
│   └── ...
├── commands/           # CLI 명령어 (crawl, download, auth 등)
└── cli.py              # 진입점 (Entrypoint)
```

## 🤝 기여하기 (Contributing)

기여 가이드라인 및 커밋 컨벤션은 [agents.md](./agents.md) 파일을 참고해주세요.

## 📄 라이선스 (License)

이 프로젝트는 MIT 라이선스를 따릅니다.
