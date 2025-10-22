# core/components/krx_processor.py
import pandas as pd
import io
from typing import List

def process_krx_net_value_excel(excel_bytes: bytes) -> pd.DataFrame:
    """
    KRX에서 다운로드한 원본 엑셀 바이트(bytes)를 파싱하여
    순매수대금 상위 20개 DataFrame으로 가공합니다.
    (순수 로직. I/O 없음)
    
    Args:
        excel_bytes (bytes): Adapter가 반환한 원본 엑셀 파일(bytes)

    Returns:
        pd.DataFrame: 가공된 상위 20개 DataFrame
    """
    
    if not excel_bytes:
        print("  [Component] ⚠️  입력된 데이터(bytes)가 비어있어 빈 DF 반환.")
        return pd.DataFrame()

    try:
        df = pd.read_excel(io.BytesIO(excel_bytes))
    except Exception as e:
        print(f"  [Component] 🚨 엑셀 파싱 중 오류: {e}")
        return pd.DataFrame()

    # --- 데이터 가공 (순매수 거래대금 상위 20) ---
    sort_col = None
    NET_VALUE_KEYWORDS: List[str] = ['순매수', '거래대금']
    
    for col in df.columns:
        if all(keyword in str(col).lower() for keyword in NET_VALUE_KEYWORDS):
            sort_col = col
            break
    
    if sort_col is None:
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            sort_col = numeric_cols[-1] 
            print(f"  [Component] ⚠️  순매수 컬럼을 찾을 수 없어 '{sort_col}' 기준으로 정렬.")
        else:
            print("  [Component] 🚨 유효한 숫자 컬럼이 없어 가공 실패.")
            return pd.DataFrame()

    df_sorted = df.sort_values(by=sort_col, ascending=False)
    df_top20 = df_sorted.head(20).copy() 
    
    df_final = df_top20[['종목명', sort_col]].rename(columns={sort_col: '순매수대금(천원)'})
    
    return df_final