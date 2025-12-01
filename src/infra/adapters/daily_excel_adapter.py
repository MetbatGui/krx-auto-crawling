import pandas as pd
from typing import List

from core.ports.daily_report_port import DailyReportPort
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData


class DailyExcelAdapter(DailyReportPort):
    """DailyReportPort의 구현체.

    DataFrame을 일별 엑셀 파일로 저장합니다.

    Attributes:
        storages (List[StoragePort]): 파일 저장 포트 리스트.
    """
    
    NAME_MAP = {
        'KOSPI_foreigner': '코스피외국인',
        'KOSPI_institutions': '코스피기관',
        'KOSDAQ_foreigner': '코스닥외국인',
        'KOSDAQ_institutions': '코스닥기관',
    }

    def __init__(self, storages: List[StoragePort]):
        """DailyExcelAdapter 초기화.

        Args:
            storages (List[StoragePort]): StoragePort 구현체 리스트 (예: [LocalStorageAdapter, GoogleDriveAdapter]).
        """
        self.storages = storages
        # 폴더는 저장 시점에 동적으로 생성되므로 초기화 시점에는 생성하지 않음
        print(f"[Adapter:DailyExcel] 초기화 완료 (저장소 {len(self.storages)}개)")

    def save_daily_reports(self, data_list: List[KrxData]) -> None:
        """수집된 데이터 리스트를 각각의 일별 엑셀 파일로 저장합니다.

        파일명 형식: {연도}년/{월}월/{투자자구분}/<날짜><시장><투자자>순매수.xlsx

        Args:
            data_list (List[KrxData]): 저장할 KRX 데이터 리스트.
        """
        for item in data_list:
            if item.data.empty:
                print(f"  [Adapter:DailyExcel] ⚠️ {item.key} 데이터가 비어있어 저장을 건너뜁니다.")
                continue

            try:
                # 파일 이름 생성
                korean_name_part = self.NAME_MAP.get(item.key, item.key)
                
                # 폴더 구조: {연도}년/{월}월/{투자자구분}/
                year = item.date_str[:4]
                month = item.date_str[4:6]
                investor_type = "외국인" if "foreigner" in item.key else "기관"
                
                filename = f"{year}년/{month}월/{investor_type}/{item.date_str}{korean_name_part}순매수.xlsx"

                # 저장용 복사본 생성 및 포맷팅
                df_to_save = item.data.copy()
                if '거래대금_순매수' in df_to_save.columns:
                     # 쉼표 포맷팅을 위해 문자열로 변환
                    df_to_save['거래대금_순매수'] = df_to_save['거래대금_순매수'].apply(lambda x: f"{x:,}")

                # 모든 StoragePort를 통해 저장
                for storage in self.storages:
                    success = storage.save_dataframe_excel(df_to_save, path=filename, index=False)
                    if success:
                        storage_name = storage.__class__.__name__
                        print(f"  [Adapter:DailyExcel] ✅ {storage_name} 저장 완료: {filename}")

            except Exception as e:
                print(f"  [Adapter:DailyExcel] 🚨 {item.key} 저장 실패: {e}")