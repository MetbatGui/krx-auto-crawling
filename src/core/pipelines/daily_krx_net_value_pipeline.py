from typing import Optional, Dict, Any
import os # Import os

# --- 1. Infrastructure (Adapters) ---
from infra.adapters.krx_http_adapter import KrxHttpAdapter
from infra.adapters.local_storage_adapter import LocalStorageAdapter
from infra.adapters.excel_storage_adapter import ExcelStorageAdapter
# [V6] Master Report Adapter
from infra.adapters.excel_master_report_adapter import ExcelMasterAdapter
# [V7] Ranking Report Adapter (신규 추가)
from infra.adapters.excel_ranking_report_adapter import ExcelRankingReportAdapter


# --- 2. Core (Tasks) ---
from core.tasks.krx_net_value.fetch_raw_data import (
    FetchKrxNetValueTask,
    FetchKrxNetValueTaskInput
)
from core.tasks.krx_net_value.standardize_data import (
    StandardizeKrxDataTask
)
from core.tasks.krx_net_value.process_watchlist import (
    ProcessWatchlistTask
)
from core.tasks.krx_net_value.upload_watchlist import (
    UploadWatchlistTask
)
from core.tasks.krx_net_value.upload_daily_reports import (
    UploadDailyReportsTask
)
# [V6] Master Report Task
from core.tasks.krx_net_value.update_master_reports import (
    UpdateMasterReportsTask
)
# [V7] Ranking Report Task (신규 추가)
from core.tasks.krx_net_value.update_ranking_report import (
    UpdateRankingReportTask
)


class DailyKrxNetValuePipeline:
    """
    일별 KRX 순매수 데이터 수집, 가공, 파일 저장을
    순차적으로 실행하는 파이프라인 (공유 컨텍스트 방식).

    [V7] 7번째 Task로 '일별 수급 순위 정리표' 업데이트 추가
    """

    def __init__(self, output_base_path: str = "output"):
        """
        파이프라인 초기화 시, 필요한 모든 'Adapter'와 'Task'를
        미리 생성하고 의존성을 주입(DI)합니다.

        Args:
            output_base_path (str): 모든 산출물이 저장될 루트 디렉터리.
        """

        # 1. Adapters 생성
        krx_port_adapter = KrxHttpAdapter()

        # (Adapter 1: HTS Watchlist용 - CSV 저장)
        # -> 'output/watchlist'
        hts_storage_adapter = LocalStorageAdapter(
            base_path=output_base_path
        )

        # (Adapter 2: Daily Reports용 - XLSX 저장)
        # -> 'output/순매수'
        excel_storage_adapter = ExcelStorageAdapter(
            base_path=output_base_path
        )

        # (Adapter 3: Master Reports용 - XLSX 수정)
        # -> 'output/순매수도' (V25 Adapter가 내부적으로 처리)
        excel_master_adapter = ExcelMasterAdapter(
            base_path=output_base_path,
            file_name_prefix="2025"
        )

        # --- [V7. 신규 추가된 부분] ---
        # (Adapter 4: Ranking Report용 - XLSX 시트 복사/수정)
        # -> 'output/수급순위' (Adapter가 내부적으로 추가한다고 가정)
        ranking_report_adapter = ExcelRankingReportAdapter(
            base_path=output_base_path,
            file_name="2025일별수급순위정리표.xlsx" # 고정 파일명 전달
        )
        # ---------------------------

        # 2. Tasks 생성 및 의존성 주입
        self.fetch_task = FetchKrxNetValueTask(krx_port=krx_port_adapter)
        self.standardize_task = StandardizeKrxDataTask()
        self.watchlist_task = ProcessWatchlistTask()

        # (Task 4: Watchlist CSV 저장 Task)
        self.upload_watchlist_task = UploadWatchlistTask(
            storage_port=hts_storage_adapter
        )

        # (Task 5: Daily Reports XLSX 저장 Task)
        self.upload_reports_task = UploadDailyReportsTask(
            storage_port=excel_storage_adapter
        )

        # (Task 6: Master Reports XLSX 업데이트 Task)
        self.update_master_reports_task = UpdateMasterReportsTask(
            report_port=excel_master_adapter
        )

        # --- [V7. 신규 추가된 부분 - 주석 해제] ---
        # (Task 7: Ranking Report XLSX 업데이트 Task)
        self.update_ranking_report_task = UpdateRankingReportTask( # 주석 해제
            report_port=ranking_report_adapter,
        )
        # ---------------------------

        # 3. 실행 순서 정의
        self.pipeline_steps = [
            self.fetch_task,                # 1. KRX 데이터 수집
            self.standardize_task,          # 2. 데이터 표준화
            self.watchlist_task,            # 3. 관심종목 처리
            self.upload_watchlist_task,     # 4. HTS CSV 저장
            self.upload_reports_task,       # 5. 일일 리포트 XLSX 저장
            self.update_master_reports_task,# 6. 마스터 엑셀 파일 누적 (V25)
            self.update_ranking_report_task # 7. 일별 순위 정리표 업데이트 (주석 해제)
        ]

    def run(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        파이프라인을 '공유 컨텍스트' 방식으로 순차 실행합니다.

        각 Task의 반환값(TypedDict)이 'context'에 병합(update)됩니다.
        한 단계라도 'status'가 'error' 또는 'skipped'를 반환하면
        다음 단계로 진행하지 않고 즉시 중단합니다.

        Args:
            date_str (Optional[str]): 조회할 날짜(YYYYMMDD).
                                      None이면 Task 내부에서 오늘 날짜를 사용합니다.

        Returns:
            Dict[str, Any]: 모든 Task의 결과가 누적된 최종 'context' 딕셔너리.
        """
        print("=== 🚀 일별 KRX 수급 파이프라인 시작 ===")

        # 1. 초기 컨텍스트 생성
        initial_input: FetchKrxNetValueTaskInput = {'date_str': date_str}
        context: Dict[str, Any] = initial_input

        # 2. 파이프라인 순차 실행
        for task in self.pipeline_steps:
            task_name = task.__class__.__name__

            try:
                # 현재 context를 Task에 전달하여 실행
                task_output = task.execute(context) # type: ignore

                # Task의 결과를 context에 병합(업데이트)
                context.update(task_output)

                # 3. 실패 시 즉시 중단
                task_status = context.get('status')
                if task_status in ('error', 'skipped'):
                    print(f"     -> 🚨 [Pipeline STOP] {task_name} 실패/건너뜀.")
                    print(f"         (사유: {context.get('message')})")
                    break

            except Exception as e:
                # Task 실행 중 예측 못한 오류 발생 시
                print(f"     -> 🚨 [Pipeline CRITICAL] {task_name} 실행 중 예외 발생: {e}")
                context.update({'status': 'critical_error', 'message': str(e)})
                break

        print("=== 🏁 파이프라인 종료 ===")

        # 4. 모든 결과가 누적된 최종 컨텍스트 반환
        return context


# --- [ 테스트 실행 코드 ] ---
if __name__ == "__main__":
    print("--- 파이프라인 개별 테스트 시작 ---")

    # (현재 시간이 2025년 10월 23일 오후 1시 48분이므로, 어제 날짜로 테스트)
    TEST_DATE = "20251024"

    # 1. 파이프라인 인스턴스 생성 (루트 'output' 폴더 기준)
    pipeline = DailyKrxNetValuePipeline(output_base_path="output")

    # 2. 파이프라인 실행
    final_context = pipeline.run(date_str=TEST_DATE)

    # 3. 최종 결과 요약 출력
    print("\n--- 파이프라인 최종 결과 요약 ---")
    print(f"Status: {final_context.get('status')}")
    print(f"Message: {final_context.get('message')}")
    print("-" * 30)
    print("최종 Context Keys:")
    print(final_context.keys())

    # 최종 status와 message는 마지막 Task(UpdateRankingReportTask)의 결과가 됩니다.