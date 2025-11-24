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
    """마스터 리포트 워크플로우 오케스트레이션 서비스"""
    
    def __init__(
        self,
        storage: StoragePort,
        data_service: MasterDataService,
        workbook_adapter: MasterWorkbookAdapter,
        file_name_prefix: str = "2025"
    ):
        """
        Args:
            storage: 파일 저장/로드를 위한 StoragePort
            data_service: 데이터 처리 서비스
            workbook_adapter: 워크북 생성 어댑터
            file_name_prefix: 파일명에 사용될 연도 접두사
        """
        self.storage = storage
        self.data_service = data_service
        self.workbook_adapter = workbook_adapter
        
        # 파일 경로 설정
        self.master_subdir = "순매수도"
        year_suffix = f"({file_name_prefix})"
        self.file_map: Dict[str, str] = {
            'KOSPI_foreigner': f'코스피외국인순매수도{year_suffix}.xlsx',
            'KOSDAQ_foreigner': f'코스닥외국인순매수도{year_suffix}.xlsx',
            'KOSPI_institutions': f'코스피기관순매수도{year_suffix}.xlsx',
            'KOSDAQ_institutions': f'코스닥기관순매수도{year_suffix}.xlsx',
        }
        
        # 순매수도 디렉토리 생성
        self.storage.ensure_directory(self.master_subdir)
    
    def update_reports(self, data_list: List[KrxData]) -> Dict[str, List[str]]:
        """마스터 리포트 전체 업데이트 워크플로우
        
        Args:
            data_list: 업데이트할 KRX 데이터 리스트
            
        Returns:
            각 리포트의 Top 20 종목 딕셔너리
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
        """단일 리포트를 업데이트하고 Top 20 종목을 반환합니다."""
        file_name = self.file_map.get(report_key)
        if not file_name:
            print(f"    -> [Service:MasterReport] 🚨 알 수 없는 리포트 키: {report_key}")
            return []
        
        file_path = f"{self.master_subdir}/{file_name}"
        sheet_name = report_date.strftime('%b').upper()
        pivot_sheet_name = report_date.strftime('%m%d')
        date_int = int(report_date.strftime('%Y%m%d'))
        
        print(f"    -> [Service:MasterReport] {file_name} 업데이트 시작...")
        
        new_data = self.data_service.transform_to_excel_schema(daily_data, date_int)
        existing_data = self._load_existing_data(file_path, sheet_name)
        sheet_exists = not existing_data.empty or self.storage.path_exists(file_path)
        
        if self.data_service.check_duplicate_date(existing_data, date_int):
            new_data = pd.DataFrame(columns=self.data_service.excel_columns)
            print(f"    -> [Service:MasterReport] 데이터 추가 건너뜀 (피벗은 생성)")
        
        merged_data = self.data_service.merge_data(existing_data, new_data)
        pivot_data = self.data_service.calculate_pivot(merged_data, date_int)
        
        self.workbook_adapter.save_workbook(
            file_path, sheet_name, pivot_sheet_name,
            new_data, pivot_data, date_int, sheet_exists
        )
        
        return self.data_service.extract_top_stocks(pivot_data, top_n=20)
    
    def _load_existing_data(
        self, 
        file_path: str, 
        sheet_name: str
    ) -> pd.DataFrame:
        """기존 엑셀 데이터를 로드합니다."""
        if not self.storage.path_exists(file_path):
            print(f"    -> [Service:MasterReport] 새 파일이 생성됩니다")
            return pd.DataFrame(columns=self.data_service.excel_columns)
            
        try:
            full_path = Path(self.storage.base_path) / file_path
            
            df = pd.read_excel(
                full_path,
                sheet_name=sheet_name,
                engine='openpyxl',
                skiprows=1,
                dtype={'일자': int}
            )
            
            if not df.empty and all(col in df.columns for col in self.data_service.excel_columns):
                result = df[self.data_service.excel_columns].copy()
                print(f"    -> [Service:MasterReport] 기존 '{sheet_name}' 시트 데이터 ({len(result)}줄) 로드 완료")
                return result
            else:
                print(f"    -> [Service:MasterReport] ⚠️ {sheet_name} 시트 헤더가 손상됨")
                return pd.DataFrame(columns=self.data_service.excel_columns)
                
        except (FileNotFoundError, ValueError, KeyError) as e:
            print(f"    -> [Service:MasterReport] ⚠️ 시트가 없어 새로 생성합니다")
            return pd.DataFrame(columns=self.data_service.excel_columns)
        except Exception as e:
            print(f"    -> [Service:MasterReport] 🚨 파일 로드 실패: {e}")
            raise
