import pandas as pd
import datetime
from typing import Dict, Set, List, Optional
import os

# (pip install openpyxl)
import openpyxl
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet
# (V2) 서식 관련 임포트 제거

# 포트 임포트 (의존성)
from core.ports.excel_ranking_report_port import ExcelRankingReportPort

class ExcelRankingReportAdapter(ExcelRankingReportPort):
    """
    'ExcelRankingReportPort'의 구현체(Adapter).

    [V3.1 - 저장 경로 수정]
    'output' 폴더에 있는 '2025일별수급순위정리표.xlsx' 파일을 열어,
    마지막 시트를 템플릿으로 복사하고, 새 시트의 헤더(A5, B5)를 수정 후 저장합니다.

    # 엑셀 시트 레이アウト 가정 (필수):
    - A5: 날짜 (예: '23 日')
    - B5: 요일 (예: '목')
    """

    def __init__(self, base_path: str, file_name: str):
        # [V3.1 수정] '수급순위' 하위 폴더 제거
        # (예: 'output' 폴더 생성)
        self.output_path = base_path # 변수명 변경 (ranking_path -> output_path)
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # (예: 'output/2025일별수급순위정리표.xlsx')
        self.file_path = os.path.join(self.output_path, file_name) # 경로 조합은 유지
        self.korean_weekdays = ["월", "화", "수", "목", "금", "토", "일"]

        print(f"     -> [Adapter] ExcelRankingReportAdapter 초기화 (파일: {self.file_path})")

    def _load_workbook(self) -> Optional[Workbook]:
        """엑셀 파일을 로드합니다. 파일이 없으면 None을 반환합니다."""
        try:
            book = openpyxl.load_workbook(self.file_path)
            print(f"     -> [Adapter] 파일 로드 성공: {self.file_path}")
            return book
        except FileNotFoundError:
            print(f"     -> [Adapter] 🚨 파일을 찾을 수 없습니다: {self.file_path}")
            return None
        except Exception as e:
            print(f"     -> [Adapter] 🚨 파일 로드 중 오류 발생: {e}")
            return None

    def _find_source_sheet(self, book: Workbook) -> Optional[Worksheet]:
        """워크북의 마지막 시트를 템플릿 원본으로 찾아 반환합니다."""
        if not book.sheetnames:
            print(f"     -> [Adapter] 🚨 파일에 시트가 하나도 없습니다.")
            return None
        source_sheet = book.worksheets[-1]
        print(f"     -> [Adapter] [Task 1] 원본 템플릿 시트 '{source_sheet.title}' 찾기 성공.")
        return source_sheet

    def _copy_and_prepare_sheet(
        self,
        book: Workbook,
        source_sheet: Worksheet,
        report_date: datetime.date
    ) -> Worksheet:
        """시트를 복사하고, 새 시트 이름을 설정하며, 중복 시트를 제거합니다."""
        new_sheet_name = report_date.strftime('%m%d') # 예: '1024'

        # (안정성) 기존 시트 제거
        if new_sheet_name in book.sheetnames:
            print(f"     -> [Adapter] ⚠️ 기존 '{new_sheet_name}' 시트를 삭제합니다.")
            book.remove(book[new_sheet_name])

        # 시트 복사 및 이름 설정
        new_sheet = book.copy_worksheet(source_sheet)
        new_sheet.title = new_sheet_name
        print(f"     -> [Adapter] [Task 2] 새 시트 '{new_sheet_name}' 생성 완료.")
        return new_sheet

    def _update_sheet_headers(self, sheet: Worksheet, report_date: datetime.date):
        """새 시트의 A5(날짜)와 B5(요일) 셀을 업데이트합니다."""
        # [Task 3] A5 날짜 수정
        day_str = f"{report_date.day} 日"
        sheet['A5'] = day_str
        print(f"     -> [Adapter] [Task 3] A5 셀 수정 완료: {day_str}")

        # [Task 4] B5 요일 수정
        weekday_str = self.korean_weekdays[report_date.weekday()]
        sheet['B5'] = weekday_str
        print(f"     -> [Adapter] [Task 4] B5 셀 수정 완료: {weekday_str}")

    def _save_workbook(self, book: Workbook) -> bool:
        """워크북을 저장합니다."""
        try:
            book.save(self.file_path)
            print(f"     -> [Adapter] ✅ {self.file_path} 파일 저장 완료.")
            return True
        except Exception as e:
            print(f"     -> [Adapter] 🚨 파일 저장 중 오류 발생: {e}")
            return False

    def update_ranking_report(
        self,
        report_date: datetime.date,
        previous_date: datetime.date, # (사용 안 함)
        data_to_paste: Dict[str, pd.DataFrame], # (V3 - 사용 안 함)
        common_stocks: Dict[str, Set[str]] # (V3 - 사용 안 함)
    ) -> bool:
        """
        [V3] 워크플로우 오케스트레이션:
        파일 로드 -> 원본 찾기 -> 시트 복사/준비 -> 헤더 업데이트 -> 저장
        """
        print(f"     -> [Adapter] 일별 수급 순위표 [시트 복사] 시작 (파일: {self.file_path})")

        # 1. 파일 로드
        book = self._load_workbook()
        if book is None:
            return False # 로드 실패

        # 2. 원본 시트 찾기
        source_sheet = self._find_source_sheet(book)
        if source_sheet is None:
            return False # 원본 없음

        # 3. 시트 복사 및 준비
        try: # 시트 복사 중 예외 발생 가능성 고려
            new_sheet = self._copy_and_prepare_sheet(book, source_sheet, report_date)
        except Exception as e:
             print(f"     -> [Adapter] 🚨 시트 복사/준비 중 오류 발생: {e}")
             return False

        # 4. 헤더 업데이트
        try: # 셀 접근/쓰기 중 예외 발생 가능성 고려
             self._update_sheet_headers(new_sheet, report_date)
        except Exception as e:
            print(f"     -> [Adapter] 🚨 헤더 업데이트 중 오류 발생: {e}")
            return False

        # 5. 파일 저장
        return self._save_workbook(book)

    # (V3) _paste_and_format_data 메서드 제거됨