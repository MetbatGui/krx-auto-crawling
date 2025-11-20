# core/tasks/krx_net_value/update_master_reports.py (신규 생성)

from typing import Dict, List, Optional, TypedDict
import datetime

from core.tasks.base_task import Task
# [중요] Task는 Adapter가 아닌 Port에만 의존합니다.
from core.ports.excel_master_report_port import ExcelMasterReportPort
from core.tasks.krx_net_value.standardize_data import (
    StandardizeDataTaskOutput,
    StandardizeDataTaskInput
)

# --- (Input/Output TypedDict 정의) ---
class UpdateMasterReportsTaskInput(StandardizeDataTaskInput):
    pass

class UpdateMasterReportsTaskOutput(TypedDict):
    date_str: Optional[str]
    status: str
    message: Optional[str]
# -------------------------------------


class UpdateMasterReportsTask(Task):
    """
    표준화된 DF 딕셔너리를 받아, 'ExcelMasterReportPort'를 통해
    월별 누적 엑셀 파일에 데이터를 추가합니다.
    (I/O 책임 - 복합 수정)
    """

    def __init__(self, report_port: ExcelMasterReportPort):
        """
        Args:
            report_port (ExcelMasterReportPort): 엑셀 마스터 파일
                                                 수정/저장을 담당하는 Adapter
        """
        self.report_port = report_port
        
        # 데이터를 업데이트할 대상 키 목록
        self.report_targets = [
            'KOSPI_foreigner',
            'KOSDAQ_foreigner',
            'KOSPI_institutions',
            'KOSDAQ_institutions',
        ]

    def execute(
        self, context: UpdateMasterReportsTaskInput
    ) -> UpdateMasterReportsTaskOutput:

        print(f"--- [Task] {self.__class__.__name__} 시작 (Update Master Reports) ---")

        date_str = context.get('date_str')
        if date_str is None:
            print("  -> 🚨 date_str이 제공되지 않았습니다.")
            return UpdateMasterReportsTaskOutput(
                date_str=None, status='error', message='date_str 누락'
            )

        processed_dfs_dict = context.get('processed_dfs_dict')
        status = context.get('status')

        if status in ('error', 'skipped') or not processed_dfs_dict:
            print("  -> 🚨 이전 Task가 실패했거나 표준화된 DF가 없습니다.")
            return UpdateMasterReportsTaskOutput(
                date_str=date_str, status='skipped', message='이전 Task 실패로 건너뜀'
            )

        # Port에 전달할 날짜 객체 생성
        try:
            # (FetchTask에서 date_str이 None이 아님을 보장)
            report_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
        except (ValueError, TypeError) as e:
            print(f"  -> 🚨 date_str('{date_str}') 형식이 잘못되었습니다: {e}")
            return UpdateMasterReportsTaskOutput(
                date_str=date_str, status='error', message=f'잘못된 date_str: {date_str}'
            )

        success_files: List[str] = []
        failed_files: List[str] = []

        # Task는 각 DF를 Port에 전달하는 '조율'만 담당
        for key in self.report_targets:
            df = processed_dfs_dict.get(key)

            if df is None or df.empty:
                print(f"  -> ⚠️ {key} 데이터가 없어 건너뜁니다.")
                failed_files.append(key)
                continue

            try:
                # 2. Port(약속)를 호출해 복잡한 로직(엑셀 수정) 위임
                success = self.report_port.update_report(
                    report_key=key,
                    daily_data=df,
                    report_date=report_date
                )
                
                if success:
                    success_files.append(key)
                else:
                    print(f"  -> 🚨 {key} 업데이트 실패 (Adapter가 False 반환)")
                    failed_files.append(key)
                    
            except Exception as e:
                print(f"  -> 🚨 {key} 마스터 파일 업데이트 중 예외 발생: {e}")
                failed_files.append(key)

        # --- (최종 결과 반환 로직) ---
        if not success_files:
            return UpdateMasterReportsTaskOutput(
                date_str=date_str, status='error', message='모든 마스터 리포트 업데이트 실패'
            )
        
        message = f"업데이트 완료: {len(success_files)}개"
        if failed_files:
            message += f" (실패/건너뜀: {len(failed_files)}개)"

        return UpdateMasterReportsTaskOutput(
            date_str=date_str,
            status='partial_success' if failed_files else 'success',
            message=message
        )