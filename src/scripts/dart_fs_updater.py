import pandas as pd
from pathlib import Path
import os
import sys

# --- 1. 준비 (Setup) ---

# 경로 설정
BASE_PATH = Path("output/분기별_실적_수집")
RAW_DATA_PATH = BASE_PATH / "raw_data"
PROCESSED_DATA_PATH = BASE_PATH / "processed_data"

# 검색어 지도 (SEARCH_MAP)
SEARCH_MAP = {
    '매출액': ['매출액', '매출'],
    '영업이익': ['영업이익', '영업이익(손실)'],
    '당기순이익': ['당기순이익', '당기순이익(손실)']
}

# 시트 이름
SHEET_IS = "Data_is"
SHEET_CIS = "Data_cis"

# 관리 파일 경로
COLLECTED_LIST_PATH = PROCESSED_DATA_PATH / "collected_list.txt"
FAILED_LIST_PATH = PROCESSED_DATA_PATH / "failed_list.csv"


def load_collected_list(list_path: Path) -> set:
    """
    PROCESSED_DATA_PATH에서 collected_list.txt를 읽어 Set(집합)으로 반환합니다.
    """
    if not list_path.exists():
        print(f"[정보] {list_path.name} 파일 없음. 새 목록을 시작합니다.")
        return set()
    
    with open(list_path, 'r', encoding='utf-8') as f:
        collected_set = {line.strip() for line in f if line.strip()}
    return collected_set


def get_file_list(raw_path: Path) -> list:
    """
    RAW_DATA_PATH에서 _RAW_FS.xlsx로 끝나는 모든 파일 목록을 반환합니다.
    """
    if not raw_path.exists():
        print(f"[오류] 원본 데이터 폴더를 찾을 수 없습니다: {raw_path}")
        sys.exit()
    
    return list(raw_path.glob("*_RAW_FS.xlsx"))


def find_values_in_sheet(sheet_df: pd.DataFrame, search_map: dict, items_to_find: list) -> dict:
    """
    DataFrame(시트) 내에서 원하는 항목(items_to_find)을 검색하여 값을 반환합니다.
    (로그 상세 추가)
    """
    found_data = {}
    
    try:
        item_col = sheet_df.iloc[:, 0].astype(str).str.strip()
        value_col = sheet_df.iloc[:, 1]
    except IndexError:
        print("    [경고] 시트의 열이 2개 미만이라 검색을 중단합니다.")
        return {}

    for std_name in items_to_find:  # '매출액', '영업이익', '당기순이익'
        search_terms = search_map[std_name]  # ['매출액', '매출']
        found_flag = False
        
        for term in search_terms:
            matches = item_col[item_col == term]
            
            if not matches.empty:
                idx = matches.index[0]
                value = value_col.loc[idx]
                
                # 숫자형 데이터로 변환 시도
                if isinstance(value, str):
                    try:
                        value_numeric = pd.to_numeric(value.replace(',', ''), errors='coerce')
                        # NaN이 아닌 경우에만 숫자형으로, 아니면 원본 문자열 유지
                        if pd.notna(value_numeric):
                            value = value_numeric
                    except Exception:
                        pass # 변환 실패 시 원본 문자열 유지
                
                print(f"    > [검색] '{std_name}' 항목을 '{term}'(으)로 찾음 (값: {value})")
                found_data[std_name] = value
                found_flag = True
                break
        
        if not found_flag:
            print(f"    > [검색] '{std_name}' 항목을 찾지 못함 (검색어: {search_terms})")
            
    return found_data


def process_single_file(file_path: Path, search_map: dict) -> dict:
    """
    하나의 _RAW_FS.xlsx 파일을 처리하여 성공 또는 실패 결과를 반환합니다.
    (로그 상세 추가)
    """
    company_name = file_path.name.split("_RAW_FS.xlsx")[0]
    extracted_data = {'매출액': None, '영업이익': None, '당기순이익': None}
    
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        # 파일 열기 실패는 심각한 오류일 수 있으므로 비고에 추가
        return {'status': 'fail', 'data': create_fail_report(company_name, {}, f"파일 열기 실패: {e}")}

    # --- 1차 시도 (Data_is) ---
    items_needed = list(extracted_data.keys())
    if SHEET_IS in xls.sheet_names:
        print(f"  [정보] '{SHEET_IS}' 시트에서 검색 시작...")
        try:
            df_is = pd.read_excel(xls, sheet_name=SHEET_IS, header=None)
            found_is = find_values_in_sheet(df_is, search_map, items_needed)
            extracted_data.update(found_is)
        except Exception as e:
            print(f"    [오류] {company_name} '{SHEET_IS}' 시트 처리 중 오류: {e}")
    else:
        print(f"  [정보] '{SHEET_IS}' 시트가 파일에 없습니다.")

    # --- 2차 시도 (Data_cis) ---
    items_needed = [k for k, v in extracted_data.items() if v is None]
    
    if items_needed: # IS에서 못 찾은 항목이 있다면
        if SHEET_CIS in xls.sheet_names:
            print(f"  [정보] '{SHEET_CIS}' 시트에서 누락 항목 ({items_needed}) 검색 시작...")
            try:
                df_cis = pd.read_excel(xls, sheet_name=SHEET_CIS, header=None)
                found_cis = find_values_in_sheet(df_cis, search_map, items_needed)
                extracted_data.update(found_cis)
            except Exception as e:
                print(f"    [오류] {company_name} '{SHEET_CIS}' 시트 처리 중 오류: {e}")
        else:
             print(f"  [정보] '{SHEET_CIS}' 시트가 파일에 없습니다. (누락 항목: {items_needed})")
    elif SHEET_IS in xls.sheet_names: # IS가 있었고, 누락 항목이 없는 경우
         print(f"  [정보] '{SHEET_IS}' 시트에서 모든 항목을 찾았습니다.")

    # --- 결과 판별 ---
    if all(v is not None for v in extracted_data.values()):
        extracted_data['종목명'] = company_name
        return {'status': 'success', 'data': extracted_data, 'filename': file_path.name}
    else:
        fail_report = create_fail_report(company_name, extracted_data)
        return {'status': 'fail', 'data': fail_report}


def create_fail_report(company_name: str, data: dict, note: str = "") -> dict:
    """
    failed_list.csv에 기록할 실패 내역 딕셔너리를 생성합니다.
    """
    report = {
        '종목명': company_name,
        '매출액추출여부': data.get('매출액') is not None,
        '영업이익추출여부': data.get('영업이익') is not None,
        '당기순이익추출여부': data.get('당기순이익') is not None,
        '비고': note
    }
    return report

def save_success_data(processed_path: Path, data: dict):
    """
    성공한 데이터를 <기업명>_PROCESSED_DATA.xlsx 파일로 저장 (또는 추가)합니다.
    (로그 상세 추가)
    """
    company_name = data.pop('종목명') # data 딕셔너리에서 종목명 제거 (열에는 불필요)
    save_path = processed_path / f"{company_name}_PROCESSED_DATA.xlsx"
    
    df_to_save = pd.DataFrame([data])
    
    try:
        if save_path.exists():
            print(f"  [저장] 기존 파일에 추가: {save_path.name}")
            # 더 간단하고 안정적인 방식: 읽고, 합치고, 덮어쓰기
            existing_df = pd.read_excel(save_path)
            combined_df = pd.concat([existing_df, df_to_save], ignore_index=True)
            # (수정) 데이터가 중복 저장되는 것을 방지하기 위해, 중복을 제거할 수 있습니다.
            # (옵션) 필요시 주석 해제: combined_df = combined_df.drop_duplicates()
            combined_df.to_excel(save_path, index=False, engine='openpyxl')
        else:
            print(f"  [저장] 새 파일로 생성: {save_path.name}")
            df_to_save.to_excel(save_path, index=False, engine='openpyxl')
    except Exception as e:
        print(f"  [오류] {company_name} 데이터 저장 실패 ({save_path.name}): {e}")


def save_management_files(collected_set: set, failed_list: list):
    """
    collected_list.txt와 failed_list.csv를 덮어쓰기 저장합니다.
    (로그 상세 추가)
    """
    # 1. 성공 목록
    print(f"\n[정보] {COLLECTED_LIST_PATH.name} 저장 중... (총 {len(collected_set)}개)")
    with open(COLLECTED_LIST_PATH, 'w', encoding='utf-8') as f:
        for filename in sorted(list(collected_set)):
            f.write(f"{filename}\n")
            
    # 2. 실패 목록
    if failed_list:
        print(f"[정보] {FAILED_LIST_PATH.name} 저장 중... (총 {len(failed_list)}개)")
        df_fail = pd.DataFrame(failed_list)
        columns = ['종목명', '매출액추출여부', '영업이익추출여부', '당기순이익추출여부', '비고']
        df_fail = df_fail.reindex(columns=columns)
        df_fail.to_csv(FAILED_LIST_PATH, index=False, encoding='utf-8-sig')
    elif FAILED_LIST_PATH.exists():
        print(f"[정보] 이번 실행에서 실패 항목 없음. 기존 {FAILED_LIST_PATH.name} 파일을 삭제합니다.")
        os.remove(FAILED_LIST_PATH)
    else:
        print(f"[정보] 이번 실행에서 실패 항목이 없습니다.")

def main():
    """
    DART 재무제표 추출기 메인 함수
    """
    print("--- DART 재무제표 추출기 (dart_fs_updater.py) 시작 ---")
    
    # 1. (준비) 결과 폴더 생성
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
    
    # 2. (로드) 기존 성공 목록 로드
    collected_set = load_collected_list(COLLECTED_LIST_PATH)
    
    # 3. (스캔 및 필터링) 작업 목록 생성
    all_files = get_file_list(RAW_DATA_PATH)
    work_list = [f for f in all_files if f.name not in collected_set]
    
    print(f"총 {len(all_files)}개 파일 중 {len(collected_set)}개 이미 처리됨.")
    print(f"신규/실패 파일 {len(work_list)}개를 대상으로 작업을 시작합니다.")
    
    # 4. (핵심 처리)
    temp_failed_data = []
    
    for i, file_path in enumerate(work_list):
        print(f"\n[{i + 1}/{len(work_list)}] {file_path.name} 처리 시작...")
        
        result = process_single_file(file_path, SEARCH_MAP)
        
        if result['status'] == 'success':
            save_success_data(PROCESSED_DATA_PATH, result['data'])
            collected_set.add(result['filename'])
            print(f"  [성공] {result['data']['종목명']} 처리 완료.")
        else: # 'fail'
            temp_failed_data.append(result['data'])
            print(f"  [실패] {result['data']['종목명']} (사유: {result['data'].get('비고', '항목 누락')})")

    # 5. (최종 저장)
    save_management_files(collected_set, temp_failed_data)
    
    print("\n--- 작업 완료 ---")
    processed_count = len(work_list) - len(temp_failed_data)
    print(f"신규 처리: {len(work_list)}건")
    print(f"  - 성공: {processed_count}건")
    print(f"  - 실패: {len(temp_failed_data)}건 (processed_data/failed_list.csv 확인)")
    print(f"누적 성공 파일: {len(collected_set)}개")

if __name__ == "__main__":
    main()