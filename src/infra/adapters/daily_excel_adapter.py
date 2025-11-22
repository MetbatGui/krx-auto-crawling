import pandas as pd
from pathlib import Path
import os
from typing import List

from core.ports.daily_report_port import DailyReportPort
from core.domain.models import KrxData

class DailyExcelAdapter(DailyReportPort):
    """
    DailyReportPort의 '엑셀(XLSX)' 구현체입니다.
    DataFrame을 지정된 경로에 .xlsx 파일로 저장합니다.
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path) / '순매수'
        try:
            os.makedirs(self.base_path, exist_ok=True)
            print(f"[Adapter:DailyExcel] 엑셀 스토리지 초기화됨 (Base: {self.base_path})")
        except OSError as e:
            print(f"[Adapter:DailyExcel] 🚨 기본 경로 생성 실패: {e}")
            raise

    def save_daily_reports(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터 리스트를 각각의 일별 엑셀 파일로 저장합니다.
        파일명 형식: <날짜><시장><투자자>순매수.xlsx (예: 20251020코스피외국인순매수.xlsx)
        """
        NAME_MAP = {
            'KOSPI_foreigner': '코스피외국인',
            'KOSPI_institutions': '코스피기관',
            'KOSDAQ_foreigner': '코스닥외국인',
            'KOSDAQ_institutions': '코스닥기관',
        }

        for item in data_list:
            if item.data.empty:
                print(f"  [Adapter:DailyExcel] ⚠️ {item.key} 데이터가 비어있어 저장을 건너뜁니다.")
                continue

            try:
                # 파일 이름 생성
                korean_name_part = NAME_MAP.get(item.key, item.key)
                filename = f"{item.date_str}{korean_name_part}순매수.xlsx"
                full_path = self.base_path / filename

                # 저장용 복사본 생성 및 포맷팅
                df_to_save = item.data.copy()
                if '거래대금_순매수' in df_to_save.columns:
                     # 쉼표 포맷팅을 위해 문자열로 변환
                    df_to_save['거래대금_순매수'] = df_to_save['거래대금_순매수'].apply(lambda x: f"{x:,}")

                df_to_save.to_excel(full_path, index=False)
                print(f"  [Adapter:DailyExcel] ✅ 저장 완료: {filename}")

            except Exception as e:
                print(f"  [Adapter:DailyExcel] 🚨 {item.key} 저장 실패: {e}")