"""
마스터 리포트 비즈니스 로직 서비스

전체 워크플로우를 오케스트레이션하고 다른 서비스/어댑터에 위임
"""
import pandas as pd
import datetime
from typing import Dict, List
from pathlib import Path

from core.ports.storage_port import StoragePort
from core.domain.models import KrxData
from core.services.master_data_service import MasterDataService
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter


class MasterReportService:
    """마스터 리포트 워크플로우 오케스트레이션 서비스.

    전체 워크플로우를 오케스트레이션하고 다른 서비스/어댑터에 위임합니다.

    Attributes:
        storage (StoragePort): 파일 저장/로드 포트.
        data_service (MasterDataService): 데이터 처리 서비스.
        workbook_adapter (MasterWorkbookAdapter): 워크북 어댑터.
        file_map (Dict[str, str]): 리포트 키와 파일명 매핑.
    """
    
    def __init__(
        self,
        source_storage: StoragePort,
        target_storages: List[StoragePort],
        data_service: MasterDataService,
        workbook_adapter: MasterWorkbookAdapter,
        file_name_prefix: str = "2025"
    ):
        """MasterReportService 초기화.

        Args:
            source_storage (StoragePort): 데이터 로드용 저장소 (예: LocalStorageAdapter).
            target_storages (List[StoragePort]): 데이터 저장용 저장소 리스트 (예: [LocalStorage, GoogleDrive]).
            data_service (MasterDataService): 데이터 처리 서비스.
            workbook_adapter (MasterWorkbookAdapter): 워크북 어댑터.
            file_name_prefix (str): 파일명 연도 접두사 (기본값: "2025").
        """
        self.source_storage = source_storage
        self.target_storages = target_storages
        self.data_service = data_service
        self.workbook_adapter = workbook_adapter
        
        # 파일명 매핑 (파일명 생성용 기본 이름)
        self.file_map: Dict[str, str] = {
            'KOSPI_foreigner': '코스피외국인순매수도',
            'KOSDAQ_foreigner': '코스닥외국인순매수도',
            'KOSPI_institutions': '코스피기관순매수도',
            'KOSDAQ_institutions': '코스닥기관순매수도',
        }
        
    def update_reports(self, data_list: List[KrxData]) -> Dict[str, List[str]]:
        """마스터 리포트 전체 업데이트 워크플로우를 실행합니다.
        
        Args:
            data_list (List[KrxData]): 업데이트할 KRX 데이터 리스트.
            
        Returns:
            Dict[str, List[str]]: 각 리포트의 Top 20 종목 딕셔너리.
        """
        print(f"[Service:MasterReport] 마스터 리포트 업데이트 시작...")
        
        top_stocks_map = {}
        
        for item in data_list:
            if item.data.empty:
                print(f"  [Service:MasterReport] ⚠️  {item.key} 데이터가 비어있어 건너뜁니다.")
                continue
            
            try:
                report_date = datetime.datetime.strptime(item.date_str, '%Y%m%d').date()
                top_stocks = self._update_single_report(item.key, item.data, report_date)
                
                if top_stocks:
                    top_stocks_map[item.key] = top_stocks
            except Exception as e:
                print(f"  [Service:MasterReport] 🚨 {item.key} 업데이트 실패: {e}")
        
        return top_stocks_map
    
    def _update_single_report(
        self,
        report_key: str,
        daily_data: pd.DataFrame,
        report_date: datetime.date
    ) -> List[str]:
        """단일 리포트를 업데이트하고 Top 20 종목을 반환합니다.
        
        Args:
            report_key (str): 리포트 키.
            daily_data (pd.DataFrame): 일별 데이터.
            report_date (datetime.date): 리포트 날짜.
            
        Returns:
            List[str]: Top 20 종목 리스트.
        """
        base_name = self.file_map.get(report_key)
        if not base_name:
            print(f"    -> [Service:MasterReport] 🚨 알 수 없는 리포트 키: {report_key}")
            return []
        
        # 동적 경로 및 파일명 생성
        # 구조: {Year}년/{Month}월/{BaseName}_{YYYYMM}.xlsx
        year = report_date.year
        month = report_date.month
        yyyymm = report_date.strftime('%Y%m')
        
        subdir = f"{year}년/{month:02d}월"
        file_name = f"{base_name}_{yyyymm}.xlsx"
        file_path = f"{subdir}/{file_name}"
        
        # 디렉토리 확인 및 생성 (타겟 저장소별)
        for storage in self.target_storages:
            storage.ensure_directory(subdir)
        
        # Locale 독립적인 월 이름 생성 (항상 JAN, FEB, ..., DEC)
        MONTH_NAMES = ["", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        sheet_name = MONTH_NAMES[report_date.month]
        pivot_sheet_name = report_date.strftime('%m%d')
        date_int = int(report_date.strftime('%Y%m%d'))
        
        print(f"    -> [Service:MasterReport] {file_name} 업데이트 시작... (경로: {subdir})")
        
        # 1. 이미 존재하는 피벗 시트 확인 (최적화)
        existing_top_stocks = self._check_existing_pivot(file_path, pivot_sheet_name)
        if existing_top_stocks is not None:
            return existing_top_stocks
        
        # 2. 데이터 업데이트 및 피벗 생성
        return self._process_update(
            file_path, 
            sheet_name, 
            pivot_sheet_name, 
            daily_data, 
            date_int
        )

    def _check_existing_pivot(self, file_path: str, pivot_sheet_name: str) -> Optional[List[str]]:
        """이미 존재하는 피벗 시트가 있는지 확인하고, 있다면 Top 20 종목을 반환합니다.
        
        Args:
            file_path (str): 파일 경로.
            pivot_sheet_name (str): 피벗 시트 이름.
            
        Returns:
            Optional[List[str]]: Top 20 종목 리스트, 없으면 None.
        """
        if not self.source_storage.path_exists(file_path):
            return None
            
        try:
            # load_dataframe을 사용하여 시트 로드 시도
            existing_pivot = self.source_storage.load_dataframe(
                file_path, 
                sheet_name=pivot_sheet_name,
                engine='openpyxl',
                header=2,
                index_col=0
            )
            
            if not existing_pivot.empty:
                print(f"    -> [Service:MasterReport] ⚠️ {pivot_sheet_name} 피벗 시트가 이미 존재하여 업데이트를 건너뜁니다.")
                return self.data_service.extract_top_stocks(existing_pivot, top_n=30)
                
        except Exception as e:
            print(f"    -> [Service:MasterReport] 피벗 시트 확인 중 오류 (무시하고 진행): {e}")
            
        return None

    def _process_update(
        self,
        file_path: str,
        sheet_name: str,
        pivot_sheet_name: str,
        daily_data: pd.DataFrame,
        date_int: int
    ) -> List[str]:
        """실제 데이터 업데이트 및 피벗 생성 로직을 수행합니다.
        
        Args:
            file_path (str): 파일 경로.
            sheet_name (str): 시트 이름.
            pivot_sheet_name (str): 피벗 시트 이름.
            daily_data (pd.DataFrame): 일별 데이터.
            date_int (int): 날짜 정수.
            
        Returns:
            List[str]: Top 20 종목 리스트.
        """
        new_data = self.data_service.transform_to_excel_schema(daily_data, date_int)
        existing_data = self._load_existing_data(file_path, sheet_name)
        sheet_exists = not existing_data.empty or self.source_storage.path_exists(file_path)
        
        if self.data_service.check_duplicate_date(existing_data, date_int):
            new_data = pd.DataFrame(columns=self.data_service.excel_columns)
            print(f"    -> [Service:MasterReport] 데이터 추가 건너뜀 (피벗은 생성)")
        
        merged_data = self.data_service.merge_data(existing_data, new_data)
        pivot_data = self.data_service.calculate_pivot(merged_data, date_int)
        
        self.workbook_adapter.save_workbook(
            file_path, sheet_name, pivot_sheet_name,
            new_data, pivot_data, date_int, sheet_exists
        )
        
        # Top 30 반환
        return self.data_service.extract_top_stocks(pivot_data, top_n=30)
    
    def _load_existing_data(
        self, 
        file_path: str, 
        sheet_name: str
    ) -> pd.DataFrame:
        """기존 엑셀 데이터를 로드합니다.
        
        Args:
            file_path (str): 파일 경로.
            sheet_name (str): 시트 이름.
            
        Returns:
            pd.DataFrame: 로드된 DataFrame.
        """
        if not self.source_storage.path_exists(file_path):
            print(f"    -> [Service:MasterReport] 새 파일이 생성됩니다")
            return pd.DataFrame(columns=self.data_service.excel_columns)
            
        try:
            df = self.source_storage.load_dataframe(
                file_path,
                sheet_name=sheet_name,
                engine='openpyxl',
                skiprows=1
            )
            
            if not df.empty and all(col in df.columns for col in self.data_service.excel_columns):
                # 데이터 전처리: 빈 행 제거 및 타입 변환
                df = df.dropna(subset=['일자'])
                
                # '일자' 컬럼이 datetime인지 확인하고, 아니면 변환 시도
                if not pd.api.types.is_datetime64_any_dtype(df['일자']):
                     df['일자'] = pd.to_datetime(df['일자'], errors='coerce')
                
                df = df.dropna(subset=['일자']) # 변환 실패(NaN) 제거
                
                result = df[self.data_service.excel_columns].copy()
                print(f"    -> [Service:MasterReport] 기존 '{sheet_name}' 시트 데이터 ({len(result)}줄) 로드 완료")
                return result
            else:
                print(f"    -> [Service:MasterReport] ⚠️ {sheet_name} 시트 헤더가 손상됨 (또는 없음)")
                return pd.DataFrame(columns=self.data_service.excel_columns)
                
        except (FileNotFoundError, ValueError, KeyError) as e:
            print(f"    -> [Service:MasterReport] ⚠️ 시트가 없어 새로 생성합니다")
            return pd.DataFrame(columns=self.data_service.excel_columns)
        except Exception as e:
            print(f"    -> [Service:MasterReport] 🚨 파일 로드 실패: {e}")
            raise
