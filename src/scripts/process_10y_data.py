import pandas as pd
import os

# --- 설정 ---
# 1. 원본 CSV 파일 경로
input_file_path = 'output/분기별_실적_수집/all_stock_data_long.csv'

# 2. 결과 엑셀 파일 경로
output_excel_file = 'financial_data_pivoted.xlsx'

# 3. CSV 파일의 컬럼명 (헤더가 없는 경우)
column_names = ['종목', '항목', '시기', '금액']

# 4. 엑셀 시트로 만들 항목 리스트
metrics_to_pivot = ['매출액', '영업이익', '당기순이익']
# --- 설정 끝 ---

def main():
    # 1. CSV 파일 읽기
    try:
        print(f"'{input_file_path}' 파일 읽기 시도...")
        df = pd.read_csv(input_file_path, header=None, names=column_names)
        print("파일 읽기 성공.")
        print(df.head())
        
    except FileNotFoundError:
        print(f"오류: '{input_file_path}' 파일을 찾을 수 없습니다.")
        print(f"현재 작업 디렉토리: {os.getcwd()}")
        return
    except pd.errors.EmptyDataError:
        print(f"오류: '{input_file_path}' 파일이 비어있습니다.")
        return
    except Exception as e:
        print(f"파일 읽기 중 오류 발생: {e}")
        return

    # 2. Excel 파일 저장을 위해 ExcelWriter 객체 생성
    with pd.ExcelWriter(output_excel_file, engine='openpyxl') as writer:
        print(f"\n'{output_excel_file}' 파일 생성 시작...")
        
        # 3. 각 항목별로 반복
        for metric in metrics_to_pivot:
            print(f"'{metric}' 시트 처리 중...")
            
            # 3-1. 해당 항목 데이터만 필터링
            df_metric = df[df['항목'] == metric]
            
            if not df_metric.empty:
                # 3-2. 데이터 피벗 (행: 종목, 열: 시기, 값: 금액)
                # aggfunc='first'는 혹시 모를 중복값(동일 종목, 동일 시기)이 있을 경우 첫 번째 값을 사용
                df_pivot = df_metric.pivot_table(index='종목', 
                                                 columns='시기', 
                                                 values='금액',
                                                 aggfunc='first')
                
                # 3-3. (선택 사항) '시기' 열(컬럼)을 오름차순으로 정렬
                df_pivot = df_pivot.sort_index(axis=1)

                # 3-4. Excel 시트에 저장
                df_pivot.to_excel(writer, sheet_name=metric)
                print(f"'{metric}' 시트 작성 완료.")
            else:
                print(f"'{metric}' 데이터가 없어 빈 시트를 생성합니다.")
                # 데이터가 없더라도 빈 시트를 만들어줍니다.
                pd.DataFrame().to_excel(writer, sheet_name=metric)

    print(f"\n'{output_excel_file}' 파일 저장이 완료되었습니다.")

if __name__ == "__main__":
    main()