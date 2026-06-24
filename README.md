# KRX 자동 크롤링 및 리포트 생성 시스템 (KRX Auto Crawling)

KRX(한국거래소)의 일별 거래 데이터를 자동으로 수집하고, 기관 및 외국인의 순매수 동향을 분석하여 엑셀 리포트로 생성하는 자동화 시스템입니다. 생성된 리포트는 로컬 저장소 및 구글 드라이브에 자동으로 동기화됩니다.

이 문서는 다음 개발자 또는 시스템 관리자가 프로젝트를 빠르고 정확하게 파악하고 인수인계받을 수 있도록 작성되었습니다.

---

## ✨ 주요 기능

*   **자동 데이터 수집 (KRX Native API)**: 매일 KRX에서 코스피/코스닥 시장의 기관/외국인 순매수 데이터를 수집합니다. Pykrx 의존성 없이 네이티브 API 호출 방식으로 최적화되어 있습니다.
*   **수급 분석 및 랭킹**: 특정 기간 또는 당일의 수급 주체별 순매수 상위 종목을 분석하고 랭킹을 매깁니다.
*   **신고가 지표 분석 (Performance Optimized)**:
    *   각 종목의 당일 종가를 분석하여 **역사적 신고가(역·신), 역사적 신고가 근접(역·근), 52주 신고가(52·신), 52주 신고가 근접(52·근)** 지표를 자동으로 분석하여 엑셀에 마킹합니다.
    *   벌크 가격 정보 조회(`get_bulk_price_info`) 및 네이버 금융 API를 활용해 성능이 대폭 개선되었습니다.
*   **엑셀 리포트 생성**: 가독성 높은 디자인의 일별/누적 수급 정리표 및 피벗 테이블을 생성합니다. (자동 서식 적용)
*   **클라우드 동기화 (Google Drive)**: 생성된 모든 엑셀 리포트를 지정된 구글 드라이브 공유 폴더로 자동 업로드합니다.
*   **과거 데이터 백필(Backfill)**: 지정한 시작일부터 종료일까지의 과거 데이터를 소급하여 한 번에 수집하고 리포트를 생성할 수 있습니다.

---

## 🛠 기술 스택 & 아키텍처

*   **Language**: Python 3.14
*   **Package Manager**: `uv` (가상환경 빌드 및 패키지 관리용)
*   **핵심 라이브러리**:
    *   `pandas`: 데이터프레임 처리 및 가공
    *   `playwright`: 웹 크롤링 및 네트워크 모니터링 기반 로그인 자동화
    *   `openpyxl`, `xlsxwriter`: 엑셀 시트 스타일링 및 수식 적용
    *   `google-api-python-client`: 구글 드라이브 API 연동
*   **아키텍처**: **헥사고날 아키텍처 (Hexagonal Architecture)** 적용
    *   `core/ports`에 비즈니스 규칙과 영속성/외부 서비스의 인터페이스를 선언하고, `infra/adapters`에 구체적인 구현(로컬 저장소, 구글 드라이브, KRX API 등)을 격리하여 결합도를 낮췄습니다.
*   **인프라**: Docker, GitHub Actions (크론 탭 자동화용)
*   **Task Runner**: `just` (자동화 스크립트 실행 엔진)

---

## 📂 프로젝트 폴더 구조

```text
krx-auto-crawling/
├── .agent/              # 에이전트 캐시 및 설정
├── configs/             # 서비스 동작 설정 파일
├── docker/              # Docker 환경 구축 파일 (Dockerfile, Docker Compose 등)
├── docs/                # 시스템 상세 설계 및 참고 문서
├── input/               # 프로그램 구동 시 필요한 로컬 입력 데이터 디렉토리
├── output/              # 생성된 엑셀 리포트 저장용 로컬 디렉토리
├── secrets/             # 인증 자격 증명 키 저장소 (Git 제외 대상)
│   ├── client_secret.json     # 로컬 구동용 Google OAuth 클라이언트 보안 비밀
│   └── service-account.json   # 서버/배포 구동용 Google 서비스 계정 키
├── src/                 # 소스 코드 메인
│   ├── commands/        # CLI 명령어 컨트롤러 (crawl, backfill, auth 등)
│   ├── core/            # 도메인 모델 및 핵심 비즈니스 로직
│   │   ├── domain/      # 수급 및 가격 정보 도메인 엔티티
│   │   ├── ports/       # 어댑터용 인터페이스 규격 (StoragePort, PriceDataPort 등)
│   │   └── services/    # 랭킹, 신고가 분석 등 애플리케이션 서비스
│   ├── infra/           # 외부 연동용 어댑터 구현체
│   │   ├── adapters/    # Google Drive, Local, KRX API, Excel 어댑터
│   │   └── ...
│   └── cli.py           # 전체 CLI 애플리케이션 진입점 (Entrypoint)
├── tests/               # 단위 테스트 및 통합 테스트 코드
├── pyproject.toml       # Python 패키지 의존성 정의
├── uv.lock              # uv 패키지 잠금 파일
├── justfile             # Windows용 Just 레시피 정의
└── Justfile.common      # OS 공통 Just 레시피 정의
```

---

## 🚀 환경 설정 및 설치

### 1. 사전 요구 사항
*   **Python 3.14** 이상 및 **`uv` 패키지 관리자** 설치 ([uv 설치 가이드](https://github.com/astral-sh/uv))
*   **Docker 및 Docker Compose** (컨테이너 실행용)
*   **Just** 설치 (선택 사항이나, 강력 권장)

### 2. 패키지 설치 (로컬 개발 환경 설정)
프로젝트 루트에서 다음 명령어를 실행하여 가상환경을 구성하고 패키지를 설치합니다.
```bash
# 가상환경 생성 및 의존성 다운로드/동기화
uv sync

# Playwright용 브라우저 드라이버 설치
uv run playwright install chromium
```

### 3. 환경 변수 설정 (`.env`)
프로젝트 루트에 `.env` 파일을 생성하고 다음 값을 작성합니다.
```env
# Google Drive 업로드 대상 루트 폴더 ID
GOOGLE_DRIVE_ROOT_FOLDER_ID=your_google_drive_folder_id

# KRX API 호출 주소
KRX_OTP_URL=http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd
KRX_DOWNLOAD_URL=http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd
```

### 4. 시크릿(Secrets) 설정
`secrets/` 디렉토리를 만들고 구글 API 자격 증명을 다운로드받아 위치시킵니다.
*   **`secrets/client_secret.json`**: 로컬에서 브라우저 기반 로그인창을 통해 개인 구글 드라이브 권한을 획득할 때 필요합니다. (OAuth 2.0 Desktop app 자격 증명)
*   **`secrets/service-account.json`**: GitHub Actions나 백엔드 서버 등 백그라운드 환경에서 사용자 개입 없이 API를 상시 호출할 때 필요합니다. (서비스 계정 키 JSON)

---

## 💻 주요 사용법 (Usage)

`just`가 설치되어 있는 경우 다음과 같은 축약 명령어로 시스템을 제어할 수 있습니다. 로컬 PC에 `just`가 없다면 명령어 블록 내부의 실제 명령어를 `uv run ...` 형식으로 직접 입력해야 합니다.

### 1. 구글 드라이브 로컬 OAuth 인증 (`just auth`)
로컬 컴퓨터에서 구글 드라이브 동기화를 사용하려면 최초 1회 인증 프로세스가 필요합니다.
```bash
just auth
# 실제 명령: uv run netbuy auth
```
*   명령어 실행 시 웹 브라우저 창이 열리며, 구글 계정으로 로그인한 뒤 권한을 부여하면 자동으로 프로젝트 인증 파일이 갱신됩니다.

### 2. 수급 크롤링 실행 (`just crawl`)
당일 또는 특정 날짜의 수급 데이터를 크롤링하고 리포트를 생성합니다.
```bash
# 오늘 날짜 기준 수급 데이터 크롤링 및 로컬 리포트 생성
just crawl

# 특정 날짜 기준 수급 데이터 크롤링 및 리포트 생성
just crawl 20260624

# 특정 날짜 데이터 크롤링 후 구글 드라이브 자동 업로드까지 실행
just crawl 20260624 --drive
```

### 3. 과거 데이터 범위 백필 (`just backfill`)
특정 범위의 과거 데이터를 소급하여 일괄 크롤링하고 리포트를 생성합니다.
```bash
# 2026년 5월 1일부터 2026년 6월 2일까지 데이터를 소급 수집하고 로컬 리포트 갱신
just backfill -s 20260501 -e 20260602

# 백필 결과를 구글 드라이브에도 동기화
just backfill -s 20260501 -e 20260602 --drive
```

### 4. Docker 환경 빌드 및 실행
```bash
# Docker 이미지 빌드
just build

# Docker 컨테이너 상에서 오늘 날짜 크롤링 실행
just crawl
```

---

## 🚀 릴리스 및 배포 프로세스 (`just release`)

현재 작업한 브랜치를 고용주 또는 최종 운영 저장소(`employers-netbuy`)에 배포하기 위한 자동화 명령어입니다.

1.  **배포 대상 원격 등록 (최초 1회)**
    ```bash
    git remote add employers-netbuy <배포용_GitHub_저장소_URL>
    ```
2.  **릴리스 실행**
    ```bash
    just release
    ```
    *   이 명령을 실행하면 로컬의 `main` 브랜치를 기준으로 `release` 브랜치가 생성/전환되며, `employers-netbuy`의 `main` 브랜치로 자동 강제 푸시된 후 다시 원래 `main`으로 돌아옵니다.

---

## 💡 인수인계 시 주의 사항 (개발 팁)

1.  **KRX 크롤링 트래픽 제약 (IP 차단 주의)**:
    *   KRX 공식 홈페이지 및 데이터 시스템은 과도한 호출에 대해 IP를 차단하거나 OTP 생성을 차단할 수 있습니다. 
    *   이를 방지하기 위해 `NativeKrxAdapter`에서는 세션 재사용 및 임의 딜레이가 설계되어 있습니다. 벌크 가격 조회 로직인 `get_bulk_price_info`는 네이버 가격 데이터를 혼용 및 우회하여 안전하게 대량 데이터를 가져오도록 튜닝되었습니다.
2.  **템플릿 엑셀 파일 의존성**:
    *   순위 리포트 생성 시 `input/` 디렉토리에 기본 서식 템플릿 엑셀 파일이 준비되어 있어야 합니다. 
    *   데이터가 삽입된 이후 최종본에서는 내부 연산에만 필요했던 템플릿(template) 시트가 자동으로 삭제되어 저장되도록 구현되어 있습니다.
3.  **의존성 패키지 관리 (`uv`)**:
    *   의존성 추가 시 `pip install` 대신 **`uv add <패키지명>`**을 사용하여 `pyproject.toml` 및 `uv.lock`을 자동으로 최신화해 주세요.
    *   스크립트 실행 시 항상 가상환경 컨텍스트를 유지하기 위해 **`uv run`**을 접두어로 사용해 실행해야 합니다 (예: `uv run python -m unittest`).
