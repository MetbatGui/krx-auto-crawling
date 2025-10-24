import pandas as pd
import datetime
from typing import Dict, Set, List, Optional
import os

# (pip install openpyxl)
import openpyxl
from openpyxl.worksheet.worksheet import Worksheet
# (V2) 서식 관련 임포트 제거 (PatternFill, FILL_NONE)

# 포트 임포트 (의존성)
from core.ports.excel_ranking_report_port import ExcelRankingReportPort

class ExcelRankingReportAdapter(ExcelRankingReportPort):
    """
    'ExcelRankingReportPort'의 구현체(Adapter).

    [V2 - 시트 복사/수정 우선]
    '2025일별수급순위정리표.xlsx' 파일을 열어,
    가장 마지막에 있는 시트(예: '1023')를 템플릿으로 복사하고,
    새 시트(예: '1024')의 헤더(A5, B5)만 수정한 후 저장합니다.

    # 엑셀 시트 레이아웃 가정 (필수):
    - A5: 날짜 (예: '23 日')
    - B5: 요일 (예: '목')
    """
    
    # (V2) 데이터 관련 레이아웃 정의 제거
    
    def __init__(self, base_path: str, file_name: str):
        # (예: 'output/수급순위' 폴더 생성)
        self.ranking_path = base_path
        if not os.path.exists(self.ranking_path):
            os.makedirs(self.ranking_path)
            
        # (예: 'output/수급순위/2025일별수급순위정리표.xlsx')
        self.file_path = os.path.join(self.ranking_path, file_name)
        self.korean_weekdays = ["월", "화", "수", "목", "금", "토", "일"]
        
        print(f" 	-> [Adapter] ExcelRankingReportAdapter 초기화 (파일: {self.file_path})")

    def update_ranking_report(
        self,
        report_date: datetime.date,
        previous_date: datetime.date, # (사용 안 함)
        data_to_paste: Dict[str, pd.DataFrame], # (V2 - 사용 안 함)
        common_stocks: Dict[str, Set[str]] # (V2 - 사용 안 함)
    ) -> bool:
        
        print(f" 	-> [Adapter] 일별 수급 순위표 [시트 복사] 시작 (파일: {self.file_path})")
        
        try:
            # --- 1. 파일 열기 ---
            try:
                book = openpyxl.load_workbook(self.file_path)
            except FileNotFoundError:
                print(f" 	-> [Adapter] 🚨 파일을 찾을 수 없습니다: {self.file_path}")
                return False

            new_sheet_name = report_date.strftime('%m%d') # 예: '1024'
            
            # (안정성) 만약 오늘 날짜 시트가 이미 존재하면 (재실행 시) 삭제
            if new_sheet_name in book.sheetnames:
                print(f" 	-> [Adapter] ⚠️ 기존 '{new_sheet_name}' 시트를 삭제합니다.")
                book.remove(book[new_sheet_name])
            
            # --- [Task 1] 가장 최근 시트(전일) 찾기 ---
            if not book.sheetnames:
                print(f" 	-> [Adapter] 🚨 파일에 시트가 하나도 없습니다. 템플릿을 복사할 수 없습니다.")
                return False
                
            source_sheet = book.worksheets[-1] # 마지막 시트
            print(f" 	-> [Adapter] [Task 1] 원본 템플릿 시트 '{source_sheet.title}' (마지막 시트) 찾기 성공.")

            # --- [Task 2] 시트 복사 후 당일 시트 생성 ---
            new_sheet = book.copy_worksheet(source_sheet)
            new_sheet.title = new_sheet_name
            print(f" 	-> [Adapter] [Task 2] 새 시트 '{new_sheet_name}' 생성 완료.")

            # --- [Task 3] A5 날짜 수정 ---
            day_str = f"{report_date.day} 日"
            new_sheet['A5'] = day_str
            print(f" 	-> [Adapter] [Task 3] A5 셀 수정 완료: {day_str}")

            # --- [Task 4] B5 요일 수정 ---
            weekday_str = self.korean_weekdays[report_date.weekday()]
            new_sheet['B5'] = weekday_str
            print(f" 	-> [Adapter] [Task 4] B5 셀 수정 완료: {weekday_str}")
            
            # --- [V2] 데이터 붙여넣기 및 서식 적용 로직 (제거됨) ---
            # print(f" 	-> [Adapter] [Task 5] 데이터 붙여넣기 및 서식 적용 시작...")
            # self._paste_and_format_data(...)

            # --- 6. 파일 저장 ---
            book.save(self.file_path)
            print(f" 	-> [Adapter] ✅ {self.file_path} 파일 저장 완료. (시트 복사, 헤더 수정)")
            return True

        except Exception as e:
            print(f" 	-> [Adapter] 🚨 엑셀 작업 중 심각한 오류 발생: {e}")
            return False

    # (V2) _paste_and_format_data 메서드 제거됨