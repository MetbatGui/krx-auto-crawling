import pandas as pd
from typing import List

from core.ports.daily_report_port import DailyReportPort
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData, Market, Investor


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

    def __init__(self, storages: List[StoragePort], source_storage: StoragePort = None):
        """DailyExcelAdapter 초기화.

        Args:
            storages (List[StoragePort]): StoragePort 구현체 리스트 (예: [LocalStorageAdapter, GoogleDriveAdapter]).
            source_storage (StoragePort, optional): 데이터 로드용 소스 스토리지. 없으면 storages[0] 사용.
        """
        self.storages = storages
        self.source_storage = source_storage if source_storage else storages[0]
        # 폴더는 저장 시점에 동적으로 생성되므로 초기화 시점에는 생성하지 않음
        print(f"[Adapter:DailyExcel] 초기화 완료 (저장소 {len(self.storages)}개, 소스: {self.source_storage.__class__.__name__})")

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

                # Workbook 생성 및 데이터 주입
                from openpyxl import Workbook
                from openpyxl.utils.dataframe import dataframe_to_rows
                from infra.adapters.excel.excel_formatter import ExcelFormatter
                
                wb = Workbook()
                ws = wb.active
                ws.title = "Sheet1"
                
                # 데이터프레임 행 추가 (헤더 포함)
                for r in dataframe_to_rows(df_to_save, index=False, header=True):
                    ws.append(r)
                
                # 컬럼 자동 너비 조정
                ExcelFormatter.apply_autofit(ws)
                
                # 모든 StoragePort를 통해 저장
                for storage in self.storages:
                    # save_workbook 사용
                    success = storage.save_workbook(wb, path=filename)
                    if success:
                        storage_name = storage.__class__.__name__
                        print(f"  [Adapter:DailyExcel] ✅ {storage_name} 저장 완료: {filename}")

            except Exception as e:
                print(f"  [Adapter:DailyExcel] 🚨 {item.key} 저장 실패: {e}")

    def load_daily_reports(self, date_str: str) -> List[KrxData]:
        """해당 날짜의 일별 리포트 파일들을 로드합니다.
        
        저장된 4개의 파일(코스피/코스닥 + 기관/외국인)이 모두 존재해야 성공으로 간주합니다.
        
        Args:
            date_str (str): 날짜 문자열 (YYYYMMDD).

        Returns:
            List[KrxData]: 복원된 KrxData 리스트. 하나라도 없으면 빈 리스트.
        """
        restored_data = []
        
        # 복원 대상 키 정의 (순서 무관)
        target_keys = [
            ('KOSPI_foreigner', Market.KOSPI, Investor.FOREIGNER),
            ('KOSPI_institutions', Market.KOSPI, Investor.INSTITUTIONS),
            ('KOSDAQ_foreigner', Market.KOSDAQ, Investor.FOREIGNER),
            ('KOSDAQ_institutions', Market.KOSDAQ, Investor.INSTITUTIONS),
        ]
        
        print(f"[Adapter:DailyExcel] {date_str} 파일 로드 시도 (Source: {self.source_storage.__class__.__name__})...")
        
        for key, market, investor in target_keys:
            try:
                # 파일 이름 및 경로 재구성 (저장 로직과 동일해야 함)
                korean_name_part = self.NAME_MAP.get(key, key)
                year = date_str[:4]
                month = date_str[4:6]
                investor_type = "외국인" if "foreigner" in key else "기관"
                
                filename = f"{year}년/{month}월/{investor_type}/{date_str}{korean_name_part}순매수.xlsx"
                
                # 지정된 Source Storage에서 로드
                df = self.source_storage.load_dataframe(filename)
                
                if df.empty:
                    print(f"  [Adapter:DailyExcel] ⚠️ 파일이 없습니다: {filename}")
                    return [] # 하나라도 없으면 실패 처리
                

                
                # 데이터 전처리 복원 (문자열 '1,234' -> 숫자 1234)
                if '거래대금_순매수' in df.columns:
                    # 쉼표 제거 및 숫자 변환 (Error 'coerce' -> NaN)
                    df['거래대금_순매수'] = df['거래대금_순매수'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')
                
                # KrxData 객체 생성
                krx_data = KrxData(
                    market=market,
                    investor=investor,
                    date_str=date_str,
                    data=df
                )
                restored_data.append(krx_data)
                
            except Exception as e:
                print(f"  [Adapter:DailyExcel] 🚨 파일 로드 중 오류 ({key}): {e}")
                return []

        print(f"[Adapter:DailyExcel] ✅ {len(restored_data)}개 파일 로드 및 데이터 복원 완료")
        return restored_data
