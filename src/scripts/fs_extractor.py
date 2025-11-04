import pandas as pd
import re
from typing import Dict, List, Any, Final
from pathlib import Path

# [파이써닉 1] 경로를 문자열 대신 Path 객체로 관리
DEBUG_MODE: Final[bool] = True
INPUT_PATH: Final[Path] = Path("output/분기별_실적_수집/raw_data")
OUTPUT_PATH: Final[Path] = Path("output/분기별_실적_수집/processed_data")

# [작업 추가] 억원 단위를 위한 상수 정의
ONE_HUNDRED_MILLION: Final[int] = 100_000_000

# --- [이하 함수들은 이전과 동일] ---

def extract_fs_from_excel(filepath: Path) -> pd.DataFrame:
    """
    DART 재무제표 엑셀 원본 파일에서 데이터를 추출합니다.
    ... (Docstring 동일) ...
    """
    print(f"[정보] '{filepath}' 파일에서 재무제표 데이터 추출 시도...")
    excel = pd.ExcelFile(filepath)
    sheet_name = "Data_is"
    try:
        df = pd.read_excel(excel,
                           sheet_name=sheet_name,
                           header=[0, 1],
                           skiprows=[2],
                           index_col=[0, 1, 2, 3, 4, 5, 6]
                           )
        
        df.index.names = ['D310000', 'concept_id', 'label_ko', 
                          'label_en', 'class0', 'class1', 'class2']

        print(f"[성공] '{filepath}' 파일에서 재무제표 데이터 추출 완료.")
        return df
    except Exception as e:
        print(f"[오류] '{filepath}' 파일에서 재무제표 데이터 추출 실패: {e}")
        raise

def filter_fs_by_concept_id(fs_df: pd.DataFrame, 
                          concept_ids: List[str]) -> pd.DataFrame:
    """
    멀티인덱스를 가진 재무제표 DF에서 'concept_id' 레벨을 기준으로 필터링합니다.
    ... (Docstring 동일) ...
    """
    print(f"[정보] 지정된 concept_id로 재무제표 데이터 필터링 시도...")
    try:
        idx_level_values = fs_df.index.get_level_values('concept_id')
        mask = idx_level_values.isin(concept_ids)
        filtered_df = fs_df[mask]
        
        print(f"[성공] 재무제표 데이터 필터링 완료. 필터링된 항목 수: {len(filtered_df)}")
        return filtered_df
    except Exception as e:
        print(f"[오류] 재무제표 데이터 필터링 실패: {e}")
        raise

def _prepare_fs_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    분기별 계산을 위해 DataFrame을 준비합니다. (헬퍼 함수)
    
    - 멀티인덱스 칼럼을 1레벨(날짜)로 단순화합니다.
    - 모든 데이터를 숫자로 변환합니다. (변환 불가 시 NaT/NaN 처리)
    - [수정] 모든 금액을 1억으로 나눈 뒤, 소수점을 버리고 'Int64' (정수) 타입으로 변경합니다.
    """
    try:
        df = df_raw.copy()
        original_cols = df.columns.get_level_values(0)
        df.columns = original_cols
    except Exception as e:
        print(f"[경고] 칼럼 인덱스 단순화 실패 (이미 단순화되었을 수 있음): {e}")
    
    # 1. 숫자로 변환
    df_numeric = df.apply(pd.to_numeric, errors='coerce')
    
    # 2. [수정] 정수 억원 단위로 스케일링 (// 사용)
    df_scaled_float = df_numeric // ONE_HUNDRED_MILLION
    
    # 3. [수정] 'Int64' (nullable int)로 변환
    df_scaled_int = df_scaled_float.astype('Int64')
    
    print("[정보] 모든 금액을 정수 억원 단위로 변환했습니다.")
    
    return df_scaled_int

def _get_fs_patterns() -> Dict[str, re.Pattern]:
    """
    DART 재무제표의 기간 문자열을 파싱하기 위한 정규식 패턴 딕셔너리를 반환합니다. (헬퍼 함수)
    ... (Docstring 및 코드 동일) ...
    """
    return {
        'q1': re.compile(r"(?P<year>\d{4})0101-(?P=year)0331"),
        'q2': re.compile(r"(?P<year>\d{4})0401-(?P=year)0630"),
        'q3': re.compile(r"(?P<year>\d{4})0701-(?P=year)0930"),
        'q3_cum': re.compile(r"(?P<year>\d{4})0101-(?P=year)0930"),
        'annual': re.compile(r"(?P<year>\d{4})0101-(?P=year)1231")
    }

def _classify_columns(df: pd.DataFrame, 
                    patterns: Dict[str, re.Pattern]) -> Dict[str, Dict[str, pd.Series]]:
    """
    원본 DF의 칼럼들을 'q1', 'q2', 'q3', 'q3_cum', 'annual' 5개 버킷으로 분류합니다. (헬퍼 함수)
    ... (Docstring 및 코드 동일) ...
    """
    data_buckets = {key: {} for key in patterns.keys()}
    
    for col in df.columns:
        for key, pattern in patterns.items():
            if m := pattern.match(col):
                year = m.group('year')
                
                if key in ['q1', 'q2', 'q3']:
                    q_num = key[-1] 
                    data_buckets[key][f"{year}/{q_num}Q"] = df[col]
                else: 
                    data_buckets[key][year] = df[col]
                
                break 
            
    return data_buckets

def _calculate_standalone_q4(annual_data: Dict[str, pd.Series], 
                           q3_cum_data: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
    """
    4분기(Standalone) 실적을 계산합니다. (4Q = 연간 누적 - 3분기 누적)
    ... (Docstring 및 코드 동일) ...
    """
    print("[정보] 4분기(Standalone) 데이터 계산 시작...")
    
    common_years = annual_data.keys() & q3_cum_data.keys()
    
    missing_years = annual_data.keys() - q3_cum_data.keys()
    if missing_years:
        print(f"[경고] {missing_years}년 4Q 계산 불가 (3분기 누적 데이터 없음).")

    q4_data = {
        f"{year}/4Q": annual_data[year] - q3_cum_data[year]
        for year in common_years
    }
    
    print(f"[정보] 총 {len(q4_data)}개의 4Q 데이터 계산 완료.")
    return q4_data

def _assemble_quarterly_df(q1_data: dict, q2_data: dict, q3_data: dict, 
                           q4_data: dict, index: pd.Index) -> pd.DataFrame:
    """
    계산된 1, 2, 3, 4분기 데이터를 하나의 DataFrame으로 병합합니다.
    ... (Docstring 및 코드 동일) ...
    """
    all_data = {**q1_data, **q2_data, **q3_data, **q4_data}
    
    return pd.DataFrame(all_data, index=index)

# --- [수정된 함수 1] ---
def convert_to_quarterly(df: pd.DataFrame) -> pd.DataFrame:
    """
    재무제표 원본(누적/분기 혼재)을 분기별(Standalone) 데이터로 변환합니다.

    [로직]
    1. 1Q, 2Q, 3Q는 Standalone 칼럼을 그대로 사용합니다.
    2. 4Q는 (연간 누적) - (3분기 누적)으로 계산합니다.
    3. [수정] 'YYYY/4Q', 'YYYY/3Q' ... 형태로 (최신순) 정렬하여 반환합니다.

    Args:
        df (pd.DataFrame): 필터링된 원본 재무제표 DataFrame.

    Returns:
        pd.DataFrame: 분기별(Standalone) 데이터로 정제된 DataFrame.
    """
    print("[정보] 누적 데이터를 분기별(Standalone) 데이터로 변환 시작...")
    
    original_index = df.index
    
    df_numeric_scaled_int = _prepare_fs_dataframe(df) 
    patterns = _get_fs_patterns()
    classified_data = _classify_columns(df_numeric_scaled_int, patterns) 
    
    q4_data = _calculate_standalone_q4(
        classified_data['annual'], 
        classified_data['q3_cum']
    )
    
    quarterly_df = _assemble_quarterly_df(
        classified_data['q1'], 
        classified_data['q2'], 
        classified_data['q3'], 
        q4_data,
        original_index
    )
    
    # [요청 사항 1] 칼럼을 최신순 (내림차순)으로 정렬
    quarterly_df = quarterly_df.sort_index(axis=1, ascending=False)
    
    print(f"[성공] 분기별 데이터 변환 완료. 총 {len(quarterly_df.columns)}개 분기 생성 (최신순).")
    return quarterly_df

# --- [신규 함수 2] ---
def _simplify_index_to_label(df: pd.DataFrame, 
                           level_name: str = 'label_ko') -> pd.DataFrame:
    """
    DataFrame의 멀티인덱스를 지정된 레벨(기본값: 'label_ko')로 단순화합니다.

    Args:
        df (pd.DataFrame): 멀티인덱스를 가진 DataFrame.
        level_name (str, optional): 사용할 인덱스 레벨 이름.

    Returns:
        pd.DataFrame: 단순화된 인덱스를 가진 DataFrame.
    """
    try:
        new_index = df.index.get_level_values(level_name)
        df_simplified = df.copy()
        df_simplified.index = new_index
        df_simplified.index.name = None # 인덱스 이름 제거
        print(f"[정보] 행 인덱스를 '{level_name}' 기준으로 단순화했습니다.")
        return df_simplified
    except KeyError:
        print(f"[경고] 인덱스 레벨 '{level_name}'을 찾을 수 없어 단순화에 실패했습니다.")
        return df # 실패 시 원본 반환


# --- [수정된 함수 3] ---
def main():
    """
    메인 실행 함수.
    """
    if not DEBUG_MODE:
        print("[정보] DEBUG_MODE가 False입니다. 실행을 중단합니다.")
        return

    filepath = INPUT_PATH / "삼성전자_RAW_FS.xlsx"
    
    fs_data = extract_fs_from_excel(filepath)

    # [수정] target_ids 오타 수정 (iffs- -> ifrs-)
    target_ids = [
        'ifrs-full_Revenue',        # 매출액
        'dart_OperatingIncomeLoss', # 영업이익
        'ifrs-full_ProfitLoss'      # 당기순이익
    ]
    filtered_fs = filter_fs_by_concept_id(fs_data, target_ids)
    
    print("\n--- [1. 필터링된 원본 데이터] (샘플) ---")
    print(filtered_fs.head())
    print("원본 칼럼(샘플):", filtered_fs.columns.tolist()[:5])
    
    quarterly_fs_data = convert_to_quarterly(filtered_fs)

    # [요청 사항 2] 인덱스 단순화
    final_fs_data = _simplify_index_to_label(quarterly_fs_data, level_name='label_ko')

    print("\n--- [2. 최종 분기별(Standalone) 데이터 (단위: 정수 억원, 최신순, 간략 인덱스)] ---")
    print(final_fs_data)
    
    try:
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        
        output_filepath = OUTPUT_PATH / "삼성전자_Quarterly_FS.xlsx"
        
        # [수정] 최종본(final_fs_data)을 엑셀로 저장
        final_fs_data.to_excel(output_filepath)
        
        print(f"\n[성공] 최종 분기별 데이터(정수 억원) 저장 완료: {output_filepath}")
    except Exception as e:
        print(f"\n[오류] 최종 파일 저장 실패: {e}")

# --- 실행 영역 ---
if __name__ == "__main__":
    main()