# core/pipelines/daily_krx_net_value_pipeline.py (신규)
from typing import Optional, Dict, Any

# --- 1. Infrastructure (Adapter) Import ---
# 파이프라인은 'Infra'의 존재를 아는 유일한 곳입니다.
from infra.adapters.krx_http_adapter import KrxHttpAdapter

# --- 2. Core (Tasks) Import ---
# Task 클래스와 입출력 TypedDict를 모두 import합니다.
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

class DailyKrxNetValuePipeline:
    """
    일별 KRX 순매수 데이터 수집, 가공, Watchlist 생성을
    순차적으로 실행하는 파이프라인.
    """

    def __init__(self):
        """
        파이프라인 초기화 시, 필요한 모든 'Adapter'와 'Task'를
        미리 생성하고 의존성을 주입(DI)합니다.
        """
        # 1. Adapter (Infra) 생성
        krx_port_adapter = KrxHttpAdapter()

        # 2. Tasks (Core) 생성 및 의존성 주입
        # (Task 1은 Port(Adapter)를 필요로 합니다)
        self.fetch_task = FetchKrxNetValueTask(krx_port=krx_port_adapter)
        
        # (Task 2, 3은 순수 가공 Task이므로 의존성이 필요 없습니다)
        self.standardize_task = StandardizeKrxDataTask()
        self.watchlist_task = ProcessWatchlistTask()

    def run(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        파이프라인을 실행합니다.
        Task를 순서대로 호출하며, 한 단계가 실패하면 즉시 중단합니다.

        Args:
            date_str (Optional[str]): 조회할 날짜(YYYYMMDD). 
                                      None이면 '오늘' 날짜로 실행됩니다.

        Returns:
            Dict[str, Any]: 파이프라인의 최종 결과 (마지막 Task의 Output).
        """
        print("=== 🚀 일별 KRX 수급 파이프라인 시작 ===")
        
        # --- 1. Fetch Task 실행 ---
        initial_input: FetchKrxNetValueTaskInput = {'date_str': date_str}
        fetch_output: FetchKrxNetValueTaskOutput = self.fetch_task.execute(initial_input)

        if fetch_output['status'] == 'error':
            print("  -> 🚨 [Pipeline STOP] Fetch Task 실패. 파이프라인을 중단합니다.")
            return fetch_output

        # --- 2. Standardize Task 실행 ---
        # Task 1의 Output이 Task 2의 Input이 됩니다.
        standardize_output: StandardizeDataTaskOutput = self.standardize_task.execute(fetch_output)
        
        if standardize_output['status'] in ('error', 'skipped'):
            print("  -> 🚨 [Pipeline STOP] Standardize Task 실패. 파이프라인을 중단합니다.")
            return standardize_output

        # --- 3. Process Watchlist Task 실행 ---
        # Task 2의 Output이 Task 3의 Input이 됩니다.
        watchlist_output: ProcessWatchlistTaskOutput = self.watchlist_task.execute(standardize_output)

        if watchlist_output['status'] in ('error', 'skipped'):
            print("  -> 🚨 [Pipeline STOP] Process Watchlist Task 실패. 파이프라인을 중단합니다.")
            return watchlist_output

        # --- 4. 파이프라인 성공 ---
        print("=== 🏁 파이프라인 전체 성공 ===")
        # (향후 여기에 'Upload Task' 등이 추가될 수 있습니다)
        
        # 최종 결과로 마지막 Task의 Output을 반환
        return watchlist_output


# --- [ 테스트 실행 코드 ] ---
if __name__ == "__main__":
    print("--- 파이프라인 개별 테스트 시작 ---")

    # 1. 파이프라인 인스턴스 생성
    pipeline = DailyKrxNetValuePipeline()

    # 2. 테스트 날짜 지정 (데이터가 확실히 있는 어제 날짜)
    TEST_DATE = "20251021"

    # 3. 파이프라인 실행
    final_result = pipeline.run(date_str=TEST_DATE)

    # 4. 최종 결과 요약 출력
    print("\n--- 파이프라인 최종 결과 요약 ---")
    print(f"Status: {final_result.get('status')}")
    print(f"Message: {final_result.get('message')}")
    
    if final_result.get('status') == 'success':
        watchlist_df = final_result.get('watchlist_df')
        if watchlist_df is not None:
            print(f"최종 Watchlist 종목 수: {len(watchlist_df)}")
            print(watchlist_df.head())