# infra/adapters/excel_master_adapter.py (V14 - 피벗 적용)

import pandas as pd
import datetime
from typing import Dict
import os

# (pip install openpyxl)
import openpyxl
# [V9/V13] dataframe_to_rows 임포트
from openpyxl.utils.dataframe import dataframe_to_rows

from core.ports.excel_master_report_port import ExcelMasterReportPort

class ExcelMasterAdapter(ExcelMasterReportPort):
    """
    ExcelMasterReportPort의 구현체(Adapter).
    
    [최종 로직 V14]
    1. (V11) dict-to-DataFrame 방식으로 DataFrame 생성 (NaN 버그 수정)
    2. (V9) `ws.append()`를 사용해 'OCT' 시트에 데이터 누적
    3. (V13) `OCT` 시트 전체를 읽어, 요청한 피벗 테이블을
       `1023` (일별) 시트에 덮어씀
    """

    def __init__(self, base_path: str, file_name_prefix: str = "2025"):
        # ('output/순매수도' 폴더 생성)
        self.master_path = os.path.join(base_path, "순매수도")
        if not os.path.exists(self.master_path):
            os.makedirs(self.master_path)
            
        # (파일명 형식: '...순매수도(2025).xlsx')
        year_suffix = f"({file_name_prefix})"
        self.file_map: Dict[str, str] = {
            'KOSPI_foreigner': f'코스피외국인순매수도{year_suffix}.xlsx',
            'KOSDAQ_foreigner': f'코스닥외국인순매수도{year_suffix}.xlsx',
            'KOSPI_institutions': f'코스피기관순매수도{year_suffix}.xlsx',
            'KOSDAQ_institutions': f'코스닥기관순매수도{year_suffix}.xlsx',
        }

    def update_report(
        self,
        report_key: str,
        daily_data: pd.DataFrame,
        report_date: datetime.date
    ) -> bool:
        
        file_name = self.file_map.get(report_key)
        if not file_name:
            print(f"  -> [Adapter] 🚨 '{report_key}'에 해당하는 파일명을 모릅니다.")
            return False

        file_path = os.path.join(self.master_path, file_name)
        
        # [V13] 시트 이름 정의
        # 1단계(데이터 누적) 시트: 'OCT'
        sheet_name = report_date.strftime('%b').upper()
        # 2단계(피벗 생성) 시트: '1023'
        pivot_sheet_name = report_date.strftime('%m%d') 
        
        date_str = report_date.strftime('%Y%m%d')
        date_int = int(date_str) 

        print(f"  -> [Adapter] {file_name} 파일 업데이트 시작...")
        print(f"      (1단계: '{sheet_name}' 누적, 2단계: '{pivot_sheet_name}' 피벗 생성)")


        # --- 1. [V11] 새 데이터를 엑셀 스키마로 번역 ---
        try:
            data_dict = {
                '일자': date_int, 
                '종목': daily_data['종목명'],
                '금액': pd.to_numeric(daily_data['순매수_거래대금'])
            }
            new_data_formatted = pd.DataFrame(data_dict)
            new_data_formatted = new_data_formatted[['일자', '종목', '금액']]

        except KeyError as e:
            print(f"  -> [Adapter] 🚨 'daily_data'에 필요한 컬럼이 없습니다: {e}")
            return False

        # --- 2. 기존 데이터 읽기 (Pandas, 중복 검사용) ---
        excel_columns = ['일자', '종목', '금액']
        existing_df = pd.DataFrame(columns=excel_columns)
        sheet_exists = False 
        try:
            read_df = pd.read_excel(
                file_path, 
                sheet_name=sheet_name, 
                engine='openpyxl', 
                skiprows=1,
                dtype={'일자': int}
            )
            sheet_exists = True 
            if not read_df.empty:
                if all(col in read_df.columns for col in excel_columns):
                     existing_df = read_df[excel_columns].copy()
                else:
                    print(f"  -> [Adapter] ⚠️ {sheet_name} 시트 헤더가 깨져 읽을 수 없습니다.")
                    existing_df = pd.DataFrame(columns=excel_columns)
            print(f"  -> [Adapter] 기존 '{sheet_name}' 시트 데이터 ({len(existing_df)}줄) 로드 완료.")
        except FileNotFoundError:
            print(f"  -> [Adapter] ⚠️ 새 파일 '{file_name}'이 생성됩니다.")
        except (ValueError, KeyError) as e:
            print(f"  -> [Adapter] ⚠️ 파일은 있으나 '{sheet_name}' 시트가 없어 새로 생성합니다.")
        except Exception as e:
            print(f"  -> [Adapter] 🚨 파일 로드 중 예상치 못한 오류: {e}")
            return False

        # --- 3. [V13 수정] 중복 날짜 검사 ---
        if date_int in existing_df['일자'].values: 
            print(f"  -> [Adapter] ⚠️ {date_int} 데이터가 '{sheet_name}'에 이미 존재하여 무시합니다.")
            # 1단계를 건너뛰기 위해 DF를 비움 (피벗은 2단계에서 진행)
            new_data_formatted = pd.DataFrame()
            print("      (데이터 추가는 건너뛰고, 피벗 테이블 생성(2단계)은 진행합니다.)")
        
        if not new_data_formatted.empty:
            print(f"  -> [Adapter] 새 데이터 ({len(new_data_formatted)}줄) 추가 준비...")

        # --- 4. [1단계 - V9] 엑셀에 데이터 누적 (ws.append) ---
        try:
            try:
                book = openpyxl.load_workbook(file_path)
            except FileNotFoundError:
                book = openpyxl.Workbook()
                if 'Sheet' in book.sheetnames:
                    book.remove(book['Sheet'])
            
            # (데이터가 있을 때만 1단계 실행)
            if not new_data_formatted.empty:
                if sheet_exists: 
                    ws = book[sheet_name]
                    print(f"  -> [1단계] '{sheet_name}' 시트 마지막 행({ws.max_row})에 추가합니다.")
                    
                    for row in dataframe_to_rows(new_data_formatted, index=False, header=False):
                        ws.append(row)
                        
                else:
                    ws = book.create_sheet(title=sheet_name)
                    print(f"  -> [1단계] 새 '{sheet_name}' 시트를 A2 헤더 형식으로 생성.")
                    ws.append([]) # A1
                    ws.append(list(new_data_formatted.columns)) # A2
                    for row in dataframe_to_rows(new_data_formatted, index=False, header=False):
                        ws.append(row)
                
                book.save(file_path)
                print(f"  -> [1단계] ✅ {file_name} ('{sheet_name}' 시트) 누적 저장 완료.")
            else:
                print(f"  -> [1단계] ⏭️ 데이터가 (중복 등으로) 비어있어 누적 저장을 건너뜁니다.")

        except Exception as e:
            print(f"  -> [Adapter] 🚨 [1단계] {file_name} 저장 중 예외 발생: {e}")
            return False # 1단계 실패 시 2단계 진행 불가

        # --- 5. [2단계 - V13] 피벗 테이블 생성/덮어쓰기 ---
        print(f"  -> [2단계] '{pivot_sheet_name}' 피벗 테이블 생성을 시작합니다...")
        try:
            # 1. 1단계에서 저장한 'OCT' 시트 전체를 다시 읽음
            full_data_df = pd.read_excel(
                file_path, 
                sheet_name=sheet_name, 
                engine='openpyxl', 
                skiprows=1,
                dtype={'일자': int}
            )
            
            if full_data_df.empty:
                print(f"  -> [Adapter] ⚠️ '{sheet_name}' 원본 데이터가 비어있어 피벗을 생성할 수 없습니다.")
                return True # 1단계는 성공했으므로 True 반환

            # 2. Pandas로 피벗 테이블 생성 (요청사항 반영)
            pivot_df = pd.pivot_table(
                full_data_df,
                values='금액',
                index='종목',
                columns='일자',
                aggfunc='sum'
            )

            # 3. '총계' 열 추가 (오른쪽 총계)
            pivot_df['총계'] = pivot_df.sum(axis=1)

            # 4. '총계' 기준 내림 정렬
            pivot_df_sorted = pivot_df.sort_values(by='총계', ascending=False)

            # 5. openpyxl로 파일을 다시 열어 피벗 시트 덮어쓰기
            book = openpyxl.load_workbook(file_path)
            
            if pivot_sheet_name in book.sheetnames:
                book.remove(book[pivot_sheet_name])
                
            pivot_ws = book.create_sheet(title=pivot_sheet_name)
            
            # (dataframe_to_rows는 index=True, header=True가 기본값)
            # (피벗 테이블은 A1부터 시작)
            for r in dataframe_to_rows(pivot_df_sorted, index=True, header=True):
                pivot_ws.append(r)

            book.save(file_path)
            
            print(f"  -> [2단계] ✅ {file_name} ('{pivot_sheet_name}' 시트) 피벗 저장 완료.")
            print(f" -> [Adapter] 피벗 테이블 출력 샘플:\n{pivot_df_sorted.head()}")
            return True

        except Exception as e:
            print(f"  -> [Adapter] 🚨 [2단계] {file_name} 피벗 생성/저장 중 예외 발생: {e}")
            return False # 2단계 실패