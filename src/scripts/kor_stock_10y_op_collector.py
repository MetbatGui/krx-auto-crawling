import pandas as pd
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright, Browser, Page, Playwright
import time
from bs4 import BeautifulSoup
import numpy as np
import openpyxl # Excel 저장을 위해 import 확인

# --- 1. 설정 (Configuration) ---
# 브라우저 연결 설정
CDP_ENDPOINT = "http://localhost:9222"

# 데이터 경로 설정
BASE_DIR = Path.cwd()
INPUT_DIR = BASE_DIR / "input" / "분기별_실적_수집"
OUTPUT_DIR = BASE_DIR / "output" / "분기별_실적_수집"

# 파일 및 컬럼 이름 설정
STOCK_COLUMN_NAME = "종목"
MASTER_LIST_FILE = INPUT_DIR / "stock_list.csv"
COLLECTED_LIST_FILE = OUTPUT_DIR / "collected_list.csv"
DATA_FILE = OUTPUT_DIR / "operating_profit_data.csv"
FAILED_LIST_FILE = OUTPUT_DIR / "failed_list.csv"

# 스크래핑 대상 URL 설정
DATA_TYPE = "0" # 0: 연간, 2: 분기
DATA_PERIOD = "32" # 기간
# (★) TARGET_URL_FORMAT 제거 - 검색을 통해 이동

# 검색 및 파싱 설정
SEARCH_BOX_SELECTOR = "#stock_subsearch" # 검색창 셀렉터
TABLE_SELECTOR = "div.stats > table"
TARGET_ITEMS = ["매출액", "영업이익", "당기순이익", "지배주주순이익"]

# --- 2. 핵심 기능 함수 ---

# (★) 리스트 로딩 함수들 다시 추가
def load_master_list(filepath: Path) -> set[str]:
    """[1단계] 'input'의 마스터 종목 리스트(Set)를 불러옵니다."""
    try:
        df = pd.read_csv(filepath)
        if STOCK_COLUMN_NAME not in df.columns:
             raise KeyError(f"'{STOCK_COLUMN_NAME}' 컬럼을 찾을 수 없습니다.")
        return set(df[STOCK_COLUMN_NAME].dropna().astype(str))
    except FileNotFoundError:
        print(f"[오류] 마스터 파일 '{filepath}' 없음.", file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f"[오류] 마스터 파일 컬럼 오류: {e}", file=sys.stderr)
        sys.exit(1)

def load_collected_list(filepath: Path) -> set[str]:
    """[2단계] 'output'의 기수집 리스트(Set)를 불러옵니다. 없으면 헤더 생성."""
    if not filepath.exists():
        print("[정보] 기수집 리스트 없음. 새로 시작.")
        try:
            pd.DataFrame(columns=[STOCK_COLUMN_NAME]).to_csv(filepath, index=False, encoding='utf-8-sig')
            # 데이터/실패 파일 헤더도 여기서 생성 (이미 있으면 덮어쓰지 않음)
            if not DATA_FILE.exists():
                 pd.DataFrame(columns=["종목명", "항목", "년도", "영업이익"]).to_csv(DATA_FILE, index=False, encoding='utf-8-sig')
            if not FAILED_LIST_FILE.exists():
                 pd.DataFrame(columns=[STOCK_COLUMN_NAME, "오류내용"]).to_csv(FAILED_LIST_FILE, index=False, encoding='utf-8-sig')
            return set()
        except Exception as e:
            print(f"[오류] 체크포인트 파일 생성 실패: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        df = pd.read_csv(filepath)
        if STOCK_COLUMN_NAME not in df.columns:
             print(f"[경고] 기수집 파일에 '{STOCK_COLUMN_NAME}' 컬럼 없음. 빈 리스트로 처리.", file=sys.stderr)
             return set()
        return set(df[STOCK_COLUMN_NAME].dropna().astype(str))
    except pd.errors.EmptyDataError: # 파일은 있지만 비어있는 경우
        return set()
    except Exception as e: # 그 외 읽기 오류
        print(f"[경고] 기수집 파일 읽기 오류: {e}. 빈 리스트로 처리.", file=sys.stderr)
        return set()


def get_target_list(master_set: set[str], collected_set: set[str]) -> list[str]:
    """[3단계] 마스터와 기수집 리스트 비교하여 실제 작업 대상을 반환합니다."""
    targets = master_set - collected_set
    print("-" * 30)
    print(f"총 {len(master_set)}개 중 {len(collected_set)}개 수집 완료.")
    print(f"-> {len(targets)}개 종목 수집 시작.")
    print("-" * 30)
    if not targets:
        print("수집할 종목이 없습니다.")
    return sorted(list(targets))

def connect_to_running_browser(p: Playwright, endpoint: str) -> Browser:
    # ... (이전과 동일) ...
    print(f"실행 중인 브라우저 연결 시도: {endpoint}")
    try:
        browser = p.chromium.connect_over_cdp(endpoint)
        if browser.contexts:
            context = browser.contexts[0]
            context.set_default_navigation_timeout(60000)
            context.set_default_timeout(15000)
        print("브라우저 연결 성공.")
        return browser
    except Exception as e:
        print(f"[오류] 브라우저 연결 실패: {e}. '--remote-debugging-port' 옵션 확인.", file=sys.stderr)
        sys.exit(1) # 오류 코드와 함께 종료

def get_browser_page(browser: Browser) -> Page:
    # ... (이전과 동일) ...
    if not browser.contexts:
        raise ConnectionError("연결된 브라우저에 컨텍스트가 없습니다.")
    context = browser.contexts[0]
    if not context.pages:
        # raise ConnectionError("연결된 브라우저에 열려있는 탭이 없습니다.") # 에러 대신 새 탭 열기
        print("[정보] 열려있는 탭 없음. 새 탭 생성.")
        return context.new_page()
    print("기존 탭 사용.")
    return context.pages[0]

def navigate_to_main_if_needed(page: Page, required_url_part: str = "itooza.com"):
    """현재 페이지가 아이투자가 아니면 메인으로 이동 (검색 위해)."""
    if required_url_part not in page.url:
        print(f"  > 현재 URL({page.url})이 아이투자가 아님. 메인으로 이동.")
        navigate_to_page(page, "https://itooza.com/")

def navigate_to_page(page: Page, url: str):
    # ... (이전과 동일) ...
    print(f"  > 페이지 이동 시도: {url}")
    try:
        response = page.goto(url) # 기본 타임아웃 사용
        if not response or not response.ok:
             raise ConnectionError(f"페이지 로드 실패 (상태: {response.status if response else 'N/A'})")
        print("  > 페이지 로드 완료.")
    except Exception as e:
        raise ConnectionError(f"페이지 이동 중 오류: {e}")


def search_stock_and_wait(page: Page, stock_name: str, search_selector: str):
    """검색창에 종목명을 입력하고 검색 후 stats 페이지로 이동될 때까지 대기."""
    print(f"  > UI 검색 시도: '{stock_name}'")
    try:
        # 검색창이 현재 페이지에 있는지 확인 후 없으면 메인으로 이동
        if not page.query_selector(search_selector):
             print(f"  > 현재 페이지에 검색창({search_selector}) 없음. 메인으로 이동.")
             navigate_to_page(page, "https://itooza.com/")
             page.wait_for_selector(search_selector) # 메인에서 다시 대기

        page.fill(search_selector, stock_name)
        page.press(search_selector, "Enter")
        print("  > 검색 실행. 결과 페이지 대기...")
        # stats/6자리숫자/ URL로 이동하는지 확인
        page.wait_for_url(f"**/stats/[0-9]{{6}}/**") # f-string 안 {} 주의
        print(f"  > 검색 성공. stats 페이지 이동 완료: {page.url}")
    except Exception as e:
        raise ValueError(f"'{stock_name}' 검색 또는 결과 페이지 이동 실패: {e}")


def fetch_and_parse_html(page: Page, table_selector: str) -> BeautifulSoup:
    # ... (이전과 동일) ...
    print("  > 테이블 대기 및 HTML 가져오기...")
    try:
        page.wait_for_selector(table_selector) # 기본 타임아웃 사용
        html_content = page.content()
        print("  > HTML 가져오기 성공.")
        return BeautifulSoup(html_content, 'lxml')
    except Exception as e:
        raise ValueError(f"테이블({table_selector}) 대기 또는 HTML 가져오기 실패: {e}")


def extract_table_from_soup(soup: BeautifulSoup) -> pd.DataFrame:
    # ... (이전과 동일) ...
    print("  > 테이블 파싱 시작...")
    stats_div = soup.find('div', class_='stats')
    if not stats_div:
        raise ValueError("HTML에서 'stats' div를 찾을 수 없습니다.")
    data_table = stats_div.find('table')
    if not data_table:
        raise ValueError("'stats' div에서 table 태그를 찾을 수 없습니다.")
    df_list = pd.read_html(str(data_table), index_col=0)
    raw_df = df_list[0]
    raw_df.index.name = "항목"
    print("  > DataFrame 변환 완료.")
    return raw_df


def clean_numeric_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # ... (이전과 동일) ...
    print("  > 데이터 클리닝 및 숫자 변환...")
    df_cleaned = df.copy()
    for col in df_cleaned.columns:
        cleaned_col = df_cleaned[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df_cleaned[col] = pd.to_numeric(cleaned_col, errors='coerce')
    print("  > 클리닝 완료.")
    return df_cleaned


def filter_dataframe_by_items(df: pd.DataFrame, items: list[str]) -> pd.DataFrame:
    # ... (이전과 동일) ...
    print(f"  > 항목 필터링 ({items})...")
    filtered = df[df.index.str.contains('|'.join(items), na=False)]
    filtered = filtered[~filtered.index.str.contains("률", na=False)]
    if filtered.empty:
        raise ValueError(f"테이블에서 원하는 항목({items})을 찾을 수 없습니다.")
    print(f"  > {len(filtered)}개 행 필터링 완료.")
    return filtered


def save_dataframe_to_excel(df: pd.DataFrame, filepath: Path, sheet_name: str = '실적 데이터'):
    # ... (이전과 동일) ...
    print(f"  > Excel 저장 시도: {filepath}")
    try:
        df.to_excel(filepath, index=True, sheet_name=sheet_name, engine='openpyxl')
        print(f"  > Excel 저장 완료: {filepath.name}")
    except Exception as e:
        print(f"  > [경고] Excel 저장 실패: {e}", file=sys.stderr)


# (★) 체크포인트/실패 기록 함수 추가
def add_to_checkpoint(filepath: Path, stock_name: str, column_name: str):
    """성공/실패한 종목명을 지정된 CSV 파일에 이어쓰기합니다."""
    try:
        df_to_append = pd.DataFrame([{column_name: stock_name}])
        df_to_append.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"  > [경고] 체크포인트 파일 '{filepath.name}' 쓰기 실패: {e}", file=sys.stderr)

def add_to_failed_list(filepath: Path, stock_name: str, error_message: str):
     """실패한 종목명과 오류 메시지를 CSV 파일에 이어쓰기합니다."""
     try:
         df_to_append = pd.DataFrame([{STOCK_COLUMN_NAME: stock_name, "오류내용": error_message}])
         df_to_append.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8-sig')
     except Exception as e:
         print(f"  > [경고] 실패 목록 파일 '{filepath.name}' 쓰기 실패: {e}", file=sys.stderr)

# --- 3. 메인 실행 로직 ---
def scrape_and_save_stock_data(page: Page, stock_name: str):
    """단일 종목의 데이터를 스크래핑하고 저장하는 전체 과정 (UI 검색 사용)."""
    print(f"\n--- '{stock_name}' 처리 시작 ---")
    try:
        # 1. UI 검색으로 stats 페이지 이동
        search_stock_and_wait(page, stock_name, SEARCH_BOX_SELECTOR)
        # 현재 URL은 검색 결과인 stats 페이지임 (예: .../stats/005930/2/32)

        # --- (★) /0/32 URL 강제 이동 시작 (★) ---
        print("  > 목표 URL(/0/32) 확인 및 이동...")
        current_url = page.url
        url_parts = current_url.split('/') # URL을 '/' 기준으로 분리

        # URL 구조가 '.../stats/종목코드/타입/기간' 형태인지 확인
        if len(url_parts) > 5 and url_parts[-3] == 'stats':
            stock_code = url_parts[-2] # 종목코드 추출 (예: '005930')

            # 목표 URL 생성 (/0/32)
            target_url = f"https://itooza.com/stats/{stock_code}/0/32"

            # 현재 URL과 목표 URL이 다르면 이동
            if current_url != target_url:
                print(f"  > 현재 URL({current_url})과 목표 URL({target_url})이 다름. 이동 실행.")
                navigate_to_page(page, target_url)
            else:
                print("  > 이미 목표 URL(/0/32)입니다. 이동 생략.")
        else:
            # 예상치 못한 URL 구조일 경우 경고
            print(f"  > [경고] 검색 후 URL({current_url}) 구조가 예상과 다릅니다. /0/32 이동을 건너<0xEB><0x9C><0x85>니다.")
        # --- (★) /0/32 URL 강제 이동 끝 (★) ---


        # 2. HTML 가져오기 및 파싱 (이후 로직은 동일)
        soup = fetch_and_parse_html(page, TABLE_SELECTOR)
        raw_df = extract_table_from_soup(soup)

        # 3. 데이터 클리닝 및 필터링
        cleaned_df = clean_numeric_dataframe(raw_df)
        processed_df = filter_dataframe_by_items(cleaned_df, TARGET_ITEMS)

        # 4. Excel 저장
        file_type = "연간" # (/0/32 이므로 '연간'으로 고정)
        file_name = f"{stock_name}_10년_{file_type}_실적.xlsx"
        save_path = OUTPUT_DIR / file_name
        save_dataframe_to_excel(processed_df, save_path)

        print(f"--- '{stock_name}' 처리 성공 ---")
        return True # 성공 플래그 반환

    except (ConnectionError, ValueError, Exception) as e:
        error_msg = str(e)
        print(f"  > [오류] '{stock_name}' 처리 중 문제 발생: {error_msg}", file=sys.stderr)
        add_to_failed_list(FAILED_LIST_FILE, stock_name, error_msg)
        return False # 실패 플래그 반환

def run_scraper():
    """스크레이퍼 메인 실행 함수 (리스트 기반)."""
    print("--- 스크레이퍼 시작 (리스트 기반) ---")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 작업 대상 리스트 준비
    master_stocks = load_master_list(MASTER_LIST_FILE)
    collected_stocks = load_collected_list(COLLECTED_LIST_FILE)
    target_stocks = get_target_list(master_stocks, collected_stocks)

    if not target_stocks:
        print("작업 종료.")
        return

    with sync_playwright() as p:
        browser = connect_to_running_browser(p, CDP_ENDPOINT)
        page = get_browser_page(browser)

        # --- 대상 종목 반복 처리 ---
        for i, stock_name in enumerate(target_stocks):
            print(f"\n[{i+1}/{len(target_stocks)}] 작업 진행...")
            # 현재 페이지가 아이투자인지 확인 후 필요시 메인으로 이동
            navigate_to_main_if_needed(page)

            success = scrape_and_save_stock_data(page, stock_name)

            if success:
                # 성공 시 체크포인트 기록
                add_to_checkpoint(COLLECTED_LIST_FILE, stock_name, STOCK_COLUMN_NAME)

            # 서버 부하 방지 대기
            print("\n  > 2초 대기...")
            time.sleep(2)
        # --- 반복 종료 ---

    print("\n--- 스크레이퍼 종료 ---")
    print("연결된 브라우저는 수동으로 닫아주세요.")


# --- 4. 스크립트 실행 ---
if __name__ == "__main__":
    try:
        import openpyxl
    except ImportError:
        print("[오류] 'openpyxl' 라이브러리가 필요합니다. 'uv add openpyxl' 또는 'pip install openpyxl'", file=sys.stderr)
        sys.exit(1)

    run_scraper()