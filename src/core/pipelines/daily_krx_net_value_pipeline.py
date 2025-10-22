# core/pipelines/daily_krx_net_value_pipeline.py (수정)
from typing import Optional, Dict, Any, cast

# ... (Adapter 및 Task 클래스 imports) ...
from infra.adapters.krx_http_adapter import KrxHttpAdapter
from infra.adapters.local_storage_adapter import LocalStorageAdapter

from core.tasks.krx_net_value.fetch_raw_data import (
    FetchKrxNetValueTask,
    FetchKrxNetValueTaskInput,
    FetchKrxNetValueTaskOutput
)
from core.tasks.krx_net_value.standardize_data import (
    StandardizeKrxDataTask,
    StandardizeDataTaskOutput
)
from core.tasks.krx_net_value.process_watchlist import (
    ProcessWatchlistTask,
    ProcessWatchlistTaskOutput
)
from core.tasks.krx_net_value.upload_watchlist import (
    UploadWatchlistTask,
    UploadWatchlistTaskOutput
)

class DailyKrxNetValuePipeline:
    """
    일별 KRX 순매수 데이터 파이프라인 (공유 컨텍스트 방식)
    """

    def __init__(self, output_base_path: str = "output"):
        """
        파이프라인 초기화 (Adapter 및 Task 조립)
        """
        # 1. Adapters 생성
        krx_port_adapter = KrxHttpAdapter()
        
        # [수정됨] LocalStorageAdapter 사용
        storage_adapter = LocalStorageAdapter(base_path=output_base_path)

        # 2. Tasks 생성 및 의존성 주입
        self.fetch_task = FetchKrxNetValueTask(krx_port=krx_port_adapter)
        self.standardize_task = StandardizeKrxDataTask()
        self.watchlist_task = ProcessWatchlistTask()
        self.upload_task = UploadWatchlistTask(storage_port=storage_adapter)
        
        # [수정됨] 실행 순서대로 Task 리스트 정의
        self.pipeline_steps = [
            self.fetch_task,
            self.standardize_task,
            self.watchlist_task,
            self.upload_task,
            # (향후 'Standardize된 data'를 사용하는 
            #  새 Task를 여기에 추가하기만 하면 됩니다)
        ]

    def run(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        파이프라인을 '공유 컨텍스트' 방식으로 순차 실행합니다.
        
        각 Task의 반환값(TypedDict)이 'context'에 병합(update)됩니다.
        한 단계라도 'status'가 'error' 또는 'skipped'를 반환하면
        다음 단계로 진행하지 않고 즉시 중단합니다.

        Args:
            date_str (Optional[str]): 조회할 날짜(YYYYMMDD).

        Returns:
            Dict[str, Any]: 모든 Task의 결과가 누적된 최종 'context' 딕셔너리.
        """
        print("=== 🚀 일별 KRX 수급 파이프라인 시작 ===")
        
        # 1. 초기 컨텍스트 생성
        # (FetchKrxNetValueTaskInput과 호환됨)
        context: Dict[str, Any] = {'date_str': date_str}

        # 2. 파이프라인 순차 실행
        for task in self.pipeline_steps:
            task_name = task.__class__.__name__
            
            try:
                # [핵심] 현재 context를 Task에 전달하여 실행
                # (TypedDict 덕분에 타입 체커가 호환성을 검사해 줌)
                task_output = task.execute(context) # type: ignore
                
                # [핵심] Task의 결과를 context에 병합(업데이트)
                context.update(task_output)

                # 3. 실패 시 즉시 중단
                task_status = context.get('status')
                if task_status in ('error', 'skipped'):
                    print(f"  -> 🚨 [Pipeline STOP] {task_name} 실패/건너뜀.")
                    print(f"     (사유: {context.get('message')})")
                    break
                    
            except Exception as e:
                # Task 실행 중 예측 못한 오류 발생 시
                print(f"  -> 🚨 [Pipeline CRITICAL] {task_name} 실행 중 예외 발생: {e}")
                context.update({'status': 'critical_error', 'message': str(e)})
                break

        print("=== 🏁 파이프라인 종료 ===")
        
        # 4. 모든 결과가 누적된 최종 컨텍스트 반환
        return context

# --- [ 테스트 실행 코드 ] ---
if __name__ == "__main__":
    print("--- 파이프라인 개별 테스트 시작 ---")

    pipeline = DailyKrxNetValuePipeline(output_base_path="output")
    TEST_DATE = "20251021"
    
    final_context = pipeline.run(date_str=TEST_DATE)

    print("\n--- 파이프라인 최종 결과 요약 ---")
    print(f"Status: {final_context.get('status')}")
    print(f"Message: {final_context.get('message')}")
    print("-" * 30)
    print("최종 Context Keys:")
    print(final_context.keys())
    
    # (예상되는 최종 Context Keys)
    # dict_keys(['date_str', 'status', 'raw_bytes_dict', 'message', 
    #            'processed_dfs_dict', 'watchlist_df', 'destination_path'])