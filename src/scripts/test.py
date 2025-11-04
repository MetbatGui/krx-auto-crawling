import pandas as pd
from pathlib import Path

# --- 설정 ---
# 1. 파일 경로 설정
file_path = Path("output/분기별_실적_수집/raw_data/삼성전자_RAW_FS.xlsx")

# 2. 읽어올 시트 이름
sheet_name = "Data_is"

# --- 실행 ---
try:
    # 3. 엑셀 파일의 'Data_is' 시트를 데이터프레임(df)으로 읽기
    # header=None 옵션: DART 원본 파일에 헤더(제목 행)가 따로 없으므로 첫 번째 줄부터 데이터로 읽음
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # 4. 결과 확인: 데이터프레임의 상위 5줄 출력
    print(f"--- [성공] '{file_path.name}' 파일의 '{sheet_name}' 시트를 df로 변환 ---")
    print(df)

except FileNotFoundError:
    print(f"[오류] 파일을 찾을 수 없습니다: {file_path}")
except ValueError as e:
    # ValueError는 시트 이름이 없을 때 주로 발생합니다.
    print(f"[오류] 시트를 찾을 수 없거나 엑셀 파일이 아닙니다: {e}")
except Exception as e:
    print(f"[오류] 기타 오류 발생: {e}")