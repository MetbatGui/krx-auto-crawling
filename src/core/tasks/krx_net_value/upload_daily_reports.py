from typing import Any, Dict, List, Optional, TypedDict
import pandas as pd
# import datetime  # [제거] date_str 기본값 설정 로직이 FetchTask로 이동됨

from core.tasks.base_task import Task
from core.ports.storage_port import StoragePort
from core.tasks.krx_net_value.standardize_data import StandardizeDataTaskOutput

# (Input/Output TypedDict 정의는 변경 없음)
class UploadDailyReportsTaskInput(StandardizeDataTaskOutput):
    """UploadDailyReportsTask execute 메서드의 입력을 정의합니다."""
    pass

class UploadDailyReportsTaskOutput(TypedDict):
    """UploadDailyReportsTask execute 메서드의 반환을 정의합니다."""
    date_str: Optional[str]
    status: str
    message: Optional[str]


class UploadDailyReportsTask(Task):
    """
    표준화된 DF 딕셔너리를 받아 4개의 개별 엑셀 리포트 파일로 저장합니다.
    (I/O 책임 - 저장)
    """

    def __init__(self, storage_port: StoragePort):
        """UploadDailyReportsTask를 초기화합니다.
        
        Args:
            storage_port (StoragePort): 파이프라인에서 주입해주는 
                                        StoragePort의 실제 구현체
                                        (e.g., ExcelStorageAdapter).
        """
        self.storage_port = storage_port
        
        # (기존과 동일)
        self.report_targets = {
            'KOSPI_foreigner': '코스피외국인',
            'KOSDAQ_foreigner': '코스닥외국인',
            'KOSPI_institutions': '코스피기관',
            'KOSDAQ_institutions': '코스닥기관',
        }

    def execute(self, context: UploadDailyReportsTaskInput) -> UploadDailyReportsTaskOutput:
        """Task 실행: 4개 DF를 각각 엑셀로 저장"""
        
        print(f"--- [Task] {self.__class__.__name__} 시작 (Upload Reports) ---")
        
        date_str = context.get('date_str')
        processed_dfs_dict = context.get('processed_dfs_dict')
        status = context.get('status')

        if status in ('error', 'skipped') or not processed_dfs_dict:
            print("  -> 🚨 이전 Task가 실패했거나 표준화된 DF 딕셔너리가 없습니다.")
            return UploadDailyReportsTaskOutput(
                date_str=date_str, status='skipped', message='이전 Task 실패로 건너뜀'
            )

        # --- [수정된 부분] ---
        # date_str 기본값 설정 로직 제거 (FetchTask로 이동)
        # ---------------------
        # if not date_str:
        #     date_str = datetime.date.today().strftime('%Y%m%d')
        #     print(f"  -> ⚠️ date_str가 없어 오늘 날짜({date_str})로 파일명을 지정합니다.")

        saved_files: List[str] = []
        failed_files: List[str] = []

        for key, file_suffix in self.report_targets.items():
            
            df = processed_dfs_dict.get(key)
            
            # (파일 이름 형식은 기존과 동일)
            file_name = f"{date_str}{file_suffix}순매수.xlsx"

            if df is None or df.empty:
                print(f"  -> ⚠️ {key} 데이터가 없어 '{file_name}' 생성을 건너뜁니다.")
                failed_files.append(file_name)
                continue

            try:
                # --- [수정된 부분] ---
                # '종목코드' 제거 로직은 StandardizeTask로 이동되었습니다.
                # 따라서 전달받은 df를 바로 저장합니다.
                success = self.storage_port.save(df, file_name)
                # ---------------------
                
                if success:
                    saved_files.append(file_name)
                else:
                    failed_files.append(file_name)

            except Exception as e:
                print(f"  -> 🚨 {file_name} 저장 중 예외 발생: {e}")
                failed_files.append(file_name)

        if not saved_files:
            return UploadDailyReportsTaskOutput(
                date_str=date_str, status='error', message='모든 리포트 저장 실패'
            )
        
        message = f"저장 완료: {len(saved_files)}개"
        if failed_files:
            message += f" (실패/건너뜀: {len(failed_files)}개)"

        return UploadDailyReportsTaskOutput(
            date_str=date_str,
            status='partial_success' if failed_files else 'success',
            message=message
        )