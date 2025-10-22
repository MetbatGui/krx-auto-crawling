# core/components/krx_processor.py
import pandas as pd
import io
import warnings
from typing import List

def process_krx_net_value_excel(excel_bytes: bytes) -> pd.DataFrame:
    """KRX 원본 엑셀(bytes)을 파싱하여 순매수 상위 20개 DataFrame을 생성합니다.

    이 함수는 순수(pure) 로직 컴포넌트입니다. 
    네트워크 I/O나 파일 시스템 접근을 수행하지 않습니다.
    
    엑셀 파일에서 '순매수'와 '거래대금' 키워드를 포함하는 컬럼을 찾아
    내림차순으로 정렬하고, 상위 20개 종목의 '종목코드', '종목명', '순매수대금'을
    추출합니다.

    Args:
        excel_bytes (bytes): `Adapter`가 KRX에서 다운로드한 원본 엑셀 파일.

    Returns:
        pd.DataFrame: '종목코드', '종목명', '순매수대금(천원)' 컬럼을 가진
            상위 20개 DataFrame. 파싱에 실패하거나 데이터가 없는 경우
            빈 DataFrame을 반환합니다.
    """
    
    if not excel_bytes:
        print("  [Component] ⚠️  입력된 데이터(bytes)가 비어있어 빈 DF 반환.")
        return pd.DataFrame()

    try:
        # openpyxl의 'default style' 경고는 데이터와 무관하므로 무시합니다.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning, lineno=237)
            df = pd.read_excel(io.BytesIO(excel_bytes))
            
    except Exception as e:
        # 데이터가 없는 날짜(휴장일 등)는 HTML을 반환할 수 있어 파싱에 실패합니다.
        print(f"  [Component] 🚨 엑셀 파싱 중 오류 (휴장일 가능성): {e}")
        return pd.DataFrame()

    # --- 데이터 가공 (순매수 거래대금 상위 20) ---
    sort_col = None
    NET_VALUE_KEYWORDS: List[str] = ['순매수', '거래대금']
    
    for col in df.columns:
        if all(keyword in str(col).lower() for keyword in NET_VALUE_KEYWORDS):
            sort_col = col
            break
    
    # 순매수대금 컬럼을 찾지 못한 경우, 마지막 숫자 컬럼을 사용
    if sort_col is None:
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            sort_col = numeric_cols[-1] 
            print(f"  [Component] ⚠️  순매수 컬럼을 찾을 수 없어 '{sort_col}' 기준으로 정렬.")
        else:
            print("  [Component] 🚨 유효한 숫자 컬럼이 없어 가공 실패.")
            return pd.DataFrame()

    # 필수 컬럼 존재 여부 확인
    required_cols = ['종목코드', '종목명', sort_col]
    if not all(col in df.columns for col in required_cols):
        print(f"  [Component] 🚨 필수 컬럼({required_cols})이 DF에 없습니다.")
        return pd.DataFrame()

    df_sorted = df.sort_values(by=sort_col, ascending=False)
    df_top20 = df_sorted.head(20).copy() 
    
    # 최종 컬럼 선택 및 이름 변경
    df_final = df_top20[required_cols].rename(
        columns={sort_col: '순매수_거래대금'}
    )
    
    return df_final