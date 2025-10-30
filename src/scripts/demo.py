import os
import datetime
from dotenv import load_dotenv
import dart_fss as dart
import pandas as pd

# --- 전역 설정값 (Constants) ---

YEARS_TO_COLLECT = 3
"""수집할 재무 데이터의 기간 (년) (4분기 계산을 위해 최소 2년 이상 필요)"""

TARGET_ACCOUNTS = [
    '매출액',
    '영업이익',
    '당기순이익'
]
"""손익계산서에서 추출할 *표준* 계정명 리스트"""


def setup_api():
    """DART API 키를 .env 파일에서 로드하고 라이브러리에 설정합니다.

    Returns:
        bool: API 키 설정 성공 시 True, 실패 시 False.
    """
    print("환경 변수 로드 및 API 키 설정...")
    load_dotenv()
    API_KEY = os.environ.get("DART_API_KEY")

    if API_KEY is None:
        print(" [오류] DART_API_KEY가 환경 변수에 설정되지 않았습니다.")
        print("       .env 파일을 확인하거나 Github Secrets 설정을 확인하세요.")
        return False

    try:
        dart.set_api_key(api_key=API_KEY)
        print(" [성공] DART API 키 설정 완료.")
        return True
    except Exception as e:
        print(f" [오류] DART API 키 설정 오류: {e}")
        return False

def get_corporate_list():
    """DART에 공시된 전체 기업 목록을 로드(또는 캐시에서 읽기)합니다.

    Returns:
        dart_fss.corp.CorpList | None: 
            기업 목록 객체. 실패 시 None.
    """
    try:
        print("DART 전체 기업 목록을 로드/업데이트합니다... (최대 1~2분 소요)")
        corp_list = dart.get_corp_list()
        print(" [성공] 기업 목록 로드 완료.")
        return corp_list
    except Exception as e:
        print(f" [오류] DART 기업 목록 로드 중 오류 발생: {e}")
        return None

def find_corp_code(corp_list, corp_name):
    """기업 목록(CorpList)에서 기업명으로 DART 고유번호를 검색합니다.

    Args:
        corp_list (dart_fss.corp.CorpList): get_corporate_list()에서 반환된 객체.
        corp_name (str): 찾고자 하는 기업명 (예: "삼성전자").

    Returns:
        str | None: DART 기업 고유번호. 실패 시 None.
    """
    print(f"\n--- '{corp_name}' 고유번호 검색 ---")
    try:
        corp_search = corp_list.find_by_corp_name(corp_name, exactly=True)
        
        if corp_search:
            target_corp_code = corp_search[0].corp_code
            print(f" [성공] '{corp_name}' 고유번호: {target_corp_code}")
            return target_corp_code
        else:
            print(f" [실패] '{corp_name}'의 고유번호를 찾을 수 없습니다.")
            return None
            
    except Exception as e:
        print(f" [오류] 기업 검색 중 오류: {e}")
        return None

def extract_financial_statements(corp_code, start_date):
    """DART API를 통해 특정 기업의 재무제표 원본(fs) 객체를 추출합니다.

    Args:
        corp_code (str): DART 기업 고유번호.
        start_date (str): 검색 시작일 (YYYYMMDD 형식).

    Returns:
        dart_fss.fs.FinancialStatement | None: 
            재무제표 객체. 실패 시 None.
    """
    print(f"\n--- 재무제표 추출 시작 (대상: {corp_code}) ---")
    print(f"     검색 기간: {start_date} 부터 현재까지")
    print(f"     보고서 유형: 'quarter' (분기별 데이터)")
    try:
        fs = dart.fs.extract(
            corp_code=corp_code,
            bgn_de=start_date,
            report_tp='quarter',
            separate=False,
            dataset='xbrl'
        )
        print(" [성공] 재무제표 객체(fs) 추출 완료. (반기/연간 데이터 포함된 원본)")
        return fs
    except Exception as e:
        print(f" [오류] 재무제표 추출 중 오류 발생: {e}")
        return None

def calculate_all_quarters(df_all_dates):
    """순수 분기, 누적, 연간 데이터가 포함된 DataFrame을 입력받아
    4분기 실적을 계산하고, 순수 분기(1Q, 2Q, 3Q, 4Q) 데이터만
    'YYYY.Q' 컬럼명을 가진 DataFrame으로 반환합니다.

    Args:
        df_all_dates (pd.DataFrame): 
            'label_ko'가 인덱스고, 컬럼이 원본 날짜 문자열(예: '20230101-20231231')로
            구성된 DataFrame.

    Returns:
        pd.DataFrame | None: 
            1Q, 2Q, 3Q, 4Q 데이터만 포함된 DataFrame. 실패 시 None.
    """
    print("[처리] 4분기 계산 및 순수 분기 데이터 취합을 시작합니다...")
    
    try:
        df_numeric = df_all_dates.astype(str).replace(',', '', regex=True)
        df_numeric = df_numeric.apply(pd.to_numeric, errors='coerce').fillna(0)
        print(" [처리] 데이터 클리닝 (콤마 제거 및 숫자 변환) 완료.")
    except Exception as e:
        print(f" [오류] 데이터 클리닝 중 오류 발생: {e}")
        return None

    df_quarterly = pd.DataFrame(index=df_numeric.index)
    all_cols = df_numeric.columns
    
    years = sorted(list(set([col[0:4] for col in all_cols])))

    for year in years:
        col_1q = f"{year}0101-{year}0331"
        col_2q = f"{year}0401-{year}0630"
        col_3q = f"{year}0701-{year}0930"
        col_annual = f"{year}0101-{year}1231"
        col_3q_cum = f"{year}0101-{year}0930"

        if col_1q in all_cols:
            df_quarterly[f"{year}.1"] = df_numeric[col_1q]
        if col_2q in all_cols:
            df_quarterly[f"{year}.2"] = df_numeric[col_2q]
        if col_3q in all_cols:
            df_quarterly[f"{year}.3"] = df_numeric[col_3q]

        if col_annual in all_cols and col_3q_cum in all_cols:
            df_quarterly[f"{year}.4"] = df_numeric[col_annual] - df_numeric[col_3q_cum]
            print(f" [처리] {year}년 4분기 데이터 계산 완료.")
        else:
            print(f" [알림] {year}년 4분기 데이터 계산 스킵 (연간 또는 3분기 누적 데이터 부족)")

    if df_quarterly.empty:
        print("[실패] 분기 데이터를 하나도 구성하지 못했습니다.")
        return None
        
    return df_quarterly


def process_income_statement(fs, target_accounts, corp_name_for_debug=""):
    """재무제표 객체(fs)에서 손익계산서(IS 또는 CIS)를 추출하고,
    계정명을 표준화한 뒤, 4분기를 계산하여
    최종 분기별 DataFrame (억원 단위)을 반환합니다.

    Args:
        fs (dart_fss.fs.FinancialStatement): 추출된 재무제표 객체.
        target_accounts (list): 추출할 *표준* 계정명 리스트.
        corp_name_for_debug (str, optional): 디버그 파일 저장 시 사용할 기업명.

    Returns:
        pd.DataFrame | None: 
            최종 처리된 분기별 DataFrame (억원 단위). 실패 시 None.
    """
    print("\n--- 손익계산서(IS) 처리 및 필터링 시작 ---")
    try:
        # [!!! 핵심 수정 1: 계정명 매핑 테이블 정의 !!!]
        # {표준 이름: [DART에서 사용되는 실제 이름들...]}
        ACCOUNT_NAME_MAP = {
            '매출액': ['매출액', '수익(매출액)'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '당기순이익': ['당기순이익', '당기순이익(손실)', '분기순이익', '분기순이익(손실)']
        }
        
        # 1-1. 필터링할 모든 실제 이름 리스트 생성
        # (예: ['매출액', '수익(매출액)', '영업이익', '영업이익(손실)', ...])
        all_names_to_filter = []
        for std_name in target_accounts:
            all_names_to_filter.extend(ACCOUNT_NAME_MAP.get(std_name, [std_name]))
        
        # 1-2. 역방향 매핑 테이블 생성 (실제 이름 -> 표준 이름)
        # (예: {'영업이익(손실)': '영업이익', '수익(매출액)': '매출액'})
        RENAME_MAP = {dart_name: std_name 
                      for std_name, dart_names in ACCOUNT_NAME_MAP.items() 
                      for dart_name in dart_names}

        # [핵심 2: 'is' 또는 'cis' 탐색]
        print("[처리] fs['is'](손익계산서)를 먼저 시도합니다...")
        df_is = fs['is'] 
        
        if df_is is None:
            print(" [알림] 'is'를 찾지 못했습니다. fs['cis'](포괄손익계산서)를 시도합니다...")
            df_is = fs['cis']

        if df_is is None:
            print(" [실패] fs 객체에서 'is'와 'cis'를 모두 찾지 못했습니다.")
            return None
        
        if df_is.empty:
            print(" [실패] 손익계산서(IS 또는 CIS) 데이터를 찾았으나 비어있습니다.")
            return None
        
        print(" [성공] 손익계산서(IS 또는 CIS) DataFrame을 확보했습니다.")

        # [핵심 3: 'label_ko' 컬럼 튜플 찾기]
        label_ko_col_tuple = None
        for col in df_is.columns:
            if isinstance(col, tuple) and len(col) > 1 and col[1] == 'label_ko':
                label_ko_col_tuple = col
                break
        
        if label_ko_col_tuple is None:
            print("[오류] MultiIndex 컬럼에서 'label_ko' 키를 찾지 못했습니다.")
            return None
        print(f"[처리] 'label_ko' 컬럼 ( {label_ko_col_tuple} )을 찾았습니다.")

        # [핵심 4: 'label_ko' 컬럼으로 행(Row) 필터링 (모든 이름 사용)]
        print(f"[처리] {label_ko_col_tuple} 컬럼에서 {all_names_to_filter} 항목을 필터링합니다...")
        df_filtered = df_is[df_is[label_ko_col_tuple].isin(all_names_to_filter)]

        if df_filtered.empty:
            print(f" [실패] {all_names_to_filter}에 해당하는 계정을 'label_ko' 컬럼에서 찾지 못했습니다.")
            return None
        print(f"[처리] {len(df_filtered)}개의 행 필터링 성공.")

        # [핵심 5: 컬럼 정리 및 인덱스 설정 (계산 준비 단계)]
        all_date_columns_tuples = []
        for col in df_filtered.columns:
            if isinstance(col, tuple) and '-' in str(col[0]):
                all_date_columns_tuples.append(col)
        
        if not all_date_columns_tuples:
            print("[실패] 데이터에서 날짜 컬럼을 찾지 못했습니다.")
            return None
        print(f"[처리] {len(all_date_columns_tuples)}개의 전체 기간 컬럼을 찾았습니다.")

        columns_to_keep = [label_ko_col_tuple] + all_date_columns_tuples
        df_final = df_filtered[columns_to_keep]

        new_column_names = ['label_ko'] + [col[0] for col in all_date_columns_tuples]
        df_final.columns = new_column_names
        
        # [!!! 핵심 수정 6: 이름 표준화 및 인덱스 설정 !!!]
        # 1. 'label_ko'의 값을 표준 이름으로 변경 (예: '영업이익(손실)' -> '영업이익')
        df_final['label_ko'] = df_final['label_ko'].map(RENAME_MAP)
        
        # 2. (선택적) 표준 이름으로 중복이 발생하면 첫 번째 값만 남김
        df_final = df_final.drop_duplicates(subset=['label_ko'], keep='first')
        
        # 3. 표준화된 'label_ko'를 인덱스로 설정
        df_final.set_index('label_ko', inplace=True)
        
        # 4. 최종적으로 우리가 원하는 표준 계정명(target_accounts)만 남김
        df_final = df_final.reindex(target_accounts)
        
        # [핵심 7: 4분기 계산]
        df_quarterly = calculate_all_quarters(df_final)
        
        if df_quarterly is None:
            return None

        # [핵심 8: '억원' 단위로 변환]
        try:
            print("[처리] 데이터를 '억원' 단위로 변환합니다 ( / 100,000,000 )")
            df_final_agg = (df_quarterly / 100_000_000).round(0).astype(int)
            print("[처리] '억원' 단위 변환 완료.")
            
        except Exception as e:
            print(f" [경고] '억원' 단위 변환 중 오류 발생: {e}")
            df_final_agg = df_quarterly
        
        # [핵심 9: 컬럼명(YYYY.Q) 기준으로 오름차순 정렬]
        df_final_agg.sort_index(axis=1, inplace=True)
        
        print("\n[!!! 성공 !!!] 4분기 포함, 원하는 3가지 항목을 성공적으로 추출 및 정리했습니다.")
        
        return df_final_agg

    except KeyError as e:
        print(f"\n[오류] 컬럼명 {e} 항목을 찾지 못했습니다.")
        print("       (DataFrame에 'label_ko' 컬럼이 없는 등 예상과 다른 구조입니다.)")
        return None
    except Exception as e:
        print(f"\n[오류] 데이터 처리 중 오류 발생: {e}")
        return None

def print_final_dataframe(df):
    """Pandas 옵션을 조절하여 최종 DataFrame을 콘솔에 예쁘게 출력합니다.

    Args:
        df (pd.DataFrame): 출력할 DataFrame.
    """
    if df is None or df.empty:
        print(" [알림] 처리된 DataFrame이 비어있습니다.")
        return
    
    with pd.option_context('display.width', 1000, 'display.max_columns', None):
        print(df)


def process_single_corporation(corp_name, corp_list, start_date):
    """지정된 단일 기업에 대한 재무 데이터 추출 및 처리의
    전체 파이프라인을 수행합니다.

    Args:
        corp_name (str): 처리할 기업명.
        corp_list (dart_fss.corp.CorpList): DART 전체 기업 목록 객체.
        start_date (str): 데이터 조회 시작일 (YYYYMMDD).
    """
    
    target_corp_code = find_corp_code(corp_list, corp_name)
    if target_corp_code is None:
        return 

    fs = extract_financial_statements(target_corp_code, start_date)
    if fs is None:
        print(f" [알림] '{corp_name}'의 재무제표(fs) 객체를 추출하지 못했습니다.")
        return 

    df_processed = process_income_statement(fs, TARGET_ACCOUNTS, corp_name_for_debug=corp_name)
    
    if df_processed is None:
        print(f" [알림] '{corp_name}'의 손익계산서 데이터를 처리하지 못했습니다.")
        return 

    print(f"\n--- [최종 결과: {corp_name} (단위: 억원)] ---")
    print_final_dataframe(df_processed)
    print("------------------------------------------")


def main():
    """
    전체 데이터 수집 및 처리 파이프라인을 실행합니다.
    """
    TARGET_CORP_NAMES = ["삼성전자", "SK하이닉스", "LG화학", "현대자동차", "카카오"]
    
    if not setup_api():
        return 

    corp_list = get_corporate_list()
    if corp_list is None:
        return 

    start_year = datetime.date.today().year - YEARS_TO_COLLECT
    start_date = f"{start_year}0101"
    
    for corp_name in TARGET_CORP_NAMES:
        print(f"\n=========================================")
        print(f"[{corp_name}] 처리를 시작합니다.")
        print(f"=========================================")
        try:
            process_single_corporation(corp_name, corp_list, start_date)
        except Exception as e:
            print(f" [!!!] '{corp_name}' 처리 중 예기치 않은 오류 발생: {e}")
        print(f"[{corp_name}] 처리를 완료했습니다.")

    print("\n--- 모든 작업 완료 ---")

if __name__ == "__main__":
    main()