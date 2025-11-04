import os
import datetime
from dotenv import load_dotenv
import dart_fss as dart
import pandas as pd
import asyncio

# --- 전역 설정값 (Constants) ---

YEARS_TO_COLLECT = 10
"""수집할 재무 데이터의 기간 (년)"""

BASE_INPUT_DIR = "input/분기별_실적_수집"
"""입력 파일을 보관할 기본 폴더"""
STOCK_LIST_FILE = os.path.join(BASE_INPUT_DIR, "stock_list.csv")
"""처리할 기업 목록 CSV 파일"""

BASE_OUTPUT_DIR = "output/분기별_실적_수집"
"""출력 파일을 저장할 기본 폴더"""
RAW_DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "raw_data")
"""추출한 원본 fs 엑셀 파일을 저장할 폴더"""

COLLECTED_LIST_FILE = os.path.join(RAW_DATA_DIR, "collected_list.csv")
"""수집에 성공한 기업 목록 로그 파일"""
FAILED_LIST_FILE = os.path.join(RAW_DATA_DIR, "failed_list.csv")
"""수집에 실패한 기업 목록 로그 파일"""


def setup_api():
    """(동기) DART API 키를 .env 파일에서 로드하고 라이브러리에 설정합니다.

    Returns:
        bool: API 키 설정 성공 시 True, 실패 시 False.
    """
    print("환경 변수 로드 및 API 키 설정...")
    load_dotenv()
    API_KEY = os.environ.get("DART_API_KEY")

    if API_KEY is None:
        print(" [오류] DART_API_KEY가 환경 변수에 설정되지 않았습니다.")
        return False

    try:
        dart.set_api_key(api_key=API_KEY)
        print(" [성공] DART API 키 설정 완료.")
        return True
    except Exception as e:
        print(f" [오류] DART API 키 설정 오류: {e}")
        return False

def get_corporate_list():
    """(동기) DART에 공시된 전체 기업 목록을 로드합니다.

    Raises:
        Exception: DART 기업 목록 로드에 실패할 경우.

    Returns:
        dart_fss.corp.CorpList: 기업 목록 객체.
    """
    try:
        print("DART 전체 기업 목록을 로드/업데이트합니다... (최대 1~2분 소요)")
        corp_list = dart.get_corp_list()
        print(" [성공] 기업 목록 로드 완료.")
        return corp_list
    except Exception as e:
        print(f" [오류] DART 기업 목록 로드 중 오류 발생: {e}")
        raise

def load_stock_list(filepath):
    """(동기) 지정된 경로의 CSV 파일에서 기업명 리스트를 로드합니다.

    Args:
        filepath (str): 'stock_list.csv' 파일의 전체 경로.

    Returns:
        list: 기업명 문자열의 리스트.
    """
    print(f"\n--- 기업 목록 파일 로드 ---")
    print(f" [처리] '{filepath}'에서 기업 목록을 읽습니다.")
    
    if not os.path.exists(filepath):
        print(f" [!!! 오류 !!!] 기업 목록 파일을 찾을 수 없습니다: {filepath}")
        raise FileNotFoundError(f"입력 파일 없음: {filepath}")

    try:
        # [!!!] 이전에 수정한 'utf-8-sig'를 반영
        df = pd.read_csv(filepath, header=None, encoding='utf-8-sig')
        stock_names = df.iloc[:, 0].dropna().unique().tolist()
        
        if stock_names and stock_names[0].strip() == '기업명':
             stock_names.pop(0)
             
        print(f" [성공] 총 {len(stock_names)}개의 고유 기업명을 로드했습니다.")
        print(f"       -> 예시: {stock_names[:3]}...")
        return stock_names
    except Exception as e:
        print(f" [!!! 오류 !!!] '{filepath}' 파일 로드 중 오류 발생: {e}")
        raise

def log_to_csv(filepath, data_dict):
    """(동기) 수집 결과를 CSV 파일에 한 줄 추가(append)합니다.

    Args:
        filepath (str): 저장할 CSV 파일 경로.
        data_dict (dict): 저장할 데이터.
    """
    try:
        df = pd.DataFrame([data_dict])
        file_exists = os.path.exists(filepath)
        df.to_csv(filepath, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f" [!!! 로깅 오류 !!!] '{filepath}' 파일 쓰기 실패: {e}")

# [!!!] 이 함수는 동기(Blocking) 함수입니다.
def find_and_save_fs_sync(corp_list, corp_name, start_date, save_dir):
    """(동기) 기업명을 받아 고유번호를 찾고, fs를 추출하여 엑셀로 저장합니다.
    (asyncio.to_thread로 호출될 대상 함수)

    Args:
        corp_list (dart_fss.corp.CorpList): DART 전체 기업 목록 객체.
        corp_name (str): 처리할 기업명.
        start_date (str): 데이터 조회 시작일 (YYYYMMDD).
        save_dir (str): 엑셀 파일을 저장할 디렉토리.

    Raises:
        Exception: 처리 단계 중 하나라도 실패하면 오류를 발생시킵니다.
    """
    
    # 1. 기업 고유번호 찾기
    print(f" [{corp_name}] 1. 고유번호 검색...")
    corp_search = corp_list.find_by_corp_name(corp_name, exactly=True)
    if not corp_search:
        raise Exception(f"'{corp_name}'의 고유번호를 찾을 수 없음")
    target_corp_code = corp_search[0].corp_code

    # 2. 재무제표(fs) 추출 (Blocking I/O)
    print(f" [{corp_name}] 2. 재무제표(fs) 추출 시작 (대상: {target_corp_code})...")
    fs = dart.fs.extract(
        corp_code=target_corp_code,
        bgn_de=start_date,
        report_tp='quarter',
        separate=None,  # [!!!] 핵심 수정: False -> None (연결/별도 자동 탐지)
        dataset='xbrl',
        progressbar=False
    )
    
    if fs is None:
        raise Exception("fs 객체 추출 실패 (데이터가 None임)")
    
    # 3. 엑셀 파일(fs.save) 저장 (Blocking I/O)
    print(f" [{corp_name}] 3. 원본 엑셀 파일(fs.save) 저장 시작...")
    filename = f"{corp_name}_RAW_FS.xlsx" # .xlsx 확장자 명시
    fs.save(filename=filename, path=save_dir)
    print(f"       -> [{corp_name}] 저장 완료: {os.path.join(save_dir, filename)}")

# [!!!] 이 함수는 비동기(async def) 함수입니다. (수정됨)
async def process_single_corporation_async(corp_name, corp_list, start_date, semaphore): # [!!!] semaphore 인수 추가
    """(비동기) 동기 함수인 find_and_save_fs_sync를 별도 스레드에서 실행합니다.
    Semaphore를 통해 동시 실행 수를 제어합니다.

    Args:
        corp_name (str): 처리할 기업명.
        corp_list (dart_fss.corp.CorpList): DART 전체 기업 목록 객체.
        start_date (str): 데이터 조회 시작일 (YYYYMMDD).
        semaphore (asyncio.Semaphore): 동시 실행 수를 제어할 세마포 객체.

    Returns:
        str: 성공한 기업명.
    
    Raises:
        Exception: 동기 함수 실행 중 발생한 모든 오류.
    """
    print(f"[{corp_name}] 작업 대기 중...")
    
    # [!!!] 핵심: 작업 실행 전 세마포 획득을 기다림
    async with semaphore:
        print(f"[{corp_name}] 작업 시작 (동시 실행 제어 중)...")
        
        # [!!!] 핵심: I/O가 발생하는 동기 함수를 스레드에서 실행
        await asyncio.to_thread(
            find_and_save_fs_sync,
            corp_list,
            corp_name,
            start_date,
            RAW_DATA_DIR
        )
    
    return corp_name # 성공 시 기업명 반환


# [!!!] 이 함수는 비동기(async def) 함수입니다.
async def main():
    """
    워크플로우 1~5를 비동기 병렬로 실행하는 메인 지휘 함수.
    (as_completed를 사용하여 실시간 로깅)
    """
    print("--- DART 원본 재무제표(fs) 병렬 수집 스크립트 시작 ---")
    
    if not setup_api():
        return
    try:
        corp_list = get_corporate_list()
        stock_names = load_stock_list(STOCK_LIST_FILE)
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
    except Exception as e:
        print(f"\n[!!! 치명적 오류 !!!] 초기 설정 실패: {e}")
        return
    
    start_year = datetime.date.today().year - YEARS_TO_COLLECT
    start_date = f"{start_year}0101"
    
    CONCURRENT_LIMIT = 10 
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    print(f"\n--- 동시 작업 수를 {CONCURRENT_LIMIT}개로 제한합니다. ---")

    tasks = []
    
    # [!!!] 핵심 수정 1: Task를 생성할 때 기업명을 알 수 있도록 딕셔너리 사용
    # {비동기 작업(Task): 기업명}
    task_to_corp_name = {}
    
    for corp_name in stock_names:
        task = asyncio.create_task(
            process_single_corporation_async(corp_name, corp_list, start_date, semaphore)
        )
        tasks.append(task)
        task_to_corp_name[task] = corp_name # 작업 객체와 기업명 매핑

    print(f"\n--- 총 {len(tasks)}개 기업의 데이터 수집을 병렬로 시작합니다... ---")
    
    success_count = 0
    fail_count = 0

    # [!!!] 핵심 수정 2: asyncio.gather -> asyncio.as_completed
    # tasks 리스트의 작업들이 완료되는 순서대로 즉시 처리
    for task in asyncio.as_completed(tasks):
        corp_name = task_to_corp_name[task] # 완료된 작업의 기업명 찾기
        
        try:
            # 작업의 실제 결과(성공 시 기업명, 실패 시 예외)를 가져옴
            result = await task 
            
            # 4. 성공 로그 기록 (실시간)
            log_to_csv(COLLECTED_LIST_FILE, {
                'corp_name': result, # result가 성공한 기업명(corp_name)임
                'collected_at': datetime.datetime.now().isoformat()
            })
            print(f" [성공] '{result}' 처리 완료.")
            success_count += 1
            
        except Exception as e:
            # 5. 실패 로그 기록 (실시간)
            error_message = str(e).replace('\n', ' ')
            log_to_csv(FAILED_LIST_FILE, {
                'corp_name': corp_name, # 실패했으므로 매핑된 기업명 사용
                'error_message': error_message,
                'failed_at': datetime.datetime.now().isoformat()
            })
            print(f" [실패] '{corp_name}' 처리 중 오류 발생: {error_message}")
            fail_count += 1

    print("\n=========================================")
    print("--- 모든 작업 완료 ---")
    print("\n--- 요약 ---")
    print(f"  성공: {success_count} 건")
    print(f"  실패: {fail_count} 건")
    print(f"  성공 로그: {COLLECTED_LIST_FILE}")
    print(f"  실패 로그: {FAILED_LIST_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
    print("\n--- 스크립트 종료 ---\n")  