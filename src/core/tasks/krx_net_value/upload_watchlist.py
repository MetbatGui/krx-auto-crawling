from typing import Any, Dict, List, Optional, TypedDict
import pandas as pd
import datetime

from core.tasks.base_task import Task
from core.ports.storage_port import StoragePort
# Task 3의 Output이 Task 4의 Input이 됩니다.
from core.tasks.krx_net_value.process_watchlist import ProcessWatchlistTaskOutput

# 1. 입력 TypedDict (Task 3의 Output)
class UploadWatchlistTaskInput(ProcessWatchlistTaskOutput):
    """UploadWatchlistTask execute 메서드의 입력을 정의합니다."""
    pass

# 2. 출력 TypedDict (destination_path 제거)
class UploadWatchlistTaskOutput(TypedDict):
    """UploadWatchlistTask execute 메서드의 반환을 정의합니다."""
    date_str: Optional[str]
    status: str
    message: Optional[str]


class UploadWatchlistTask(Task):
    """
    Process Task의 결과(watchlist_df)를 받아 'StoragePort'에 저장(업로드)합니다.
    (I/O 책임 - 저장)
    """

    def __init__(self, storage_port: StoragePort):
        """UploadWatchlistTask를 초기화합니다.

        [의존성 주입]
        이 Task는 'StoragePort'라는 '약속(Port)'에만 의존합니다.
        
        Args:
            storage_port (StoragePort): 파이프라인에서 주입해주는 
                                        StoragePort의 실제 구현체
                                        (e.g., LocalStorageAdapter).
        """
        self.storage_port = storage_port

    def execute(self, context: UploadWatchlistTaskInput) -> UploadWatchlistTaskOutput:
        """Task 실행: Port를 호출하여 DataFrame을 저장합니다.

        Args:
            context (UploadWatchlistTaskInput): 이전 Task(ProcessWatchlist)의
                실행 결과. `watchlist_df`와 `date_str`을 포함합니다.

        Returns:
            UploadWatchlistTaskOutput: Task의 최종 실행 결과를 담은 TypedDict.
        """
        
        print(f"--- [Task] {self.__class__.__name__} 시작 (Upload) ---")
        
        date_str = context.get('date_str')
        watchlist_df = context.get('watchlist_df')
        status = context.get('status')

        # 1. 이전 Task 결과 유효성 검사
        if status in ('error', 'skipped') or watchlist_df is None or watchlist_df.empty:
            print("  -> 🚨 이전 Task(Process)가 실패했거나 Watchlist DF가 없습니다.")
            return UploadWatchlistTaskOutput(
                date_str=date_str,
                status='skipped',
                message='이전 Task 실패로 건너뜀'
            )
        
        # 2. 날짜가 없는 경우(파이프라인이 None으로 시작한 경우) 오늘 날짜로 대체
        if not date_str:
            # (참고: 현재 KST 기준 '오늘' 날짜를 사용합니다)
            today = datetime.date.today().strftime('%Y%m%d')
            print(f"  -> ⚠️ date_str가 없어 오늘 날짜({today})로 파일명을 지정합니다.")
            date_str = today
        
        # 3. 저장 위치(이름) 결정
        # (Adapter는 'output/watchlist/' 경로를 알고 있음)
        destination_name = f"{date_str}_watchlist.csv"

        try:
            # 4. Port(약속)를 통해 데이터 저장 (I/O)
            success = self.storage_port.save(watchlist_df, destination_name)
            
            if not success:
                raise Exception("Adapter의 save() 메서드가 False를 반환함")

        except Exception as e:
            print(f"  -> 🚨 {destination_name} 저장 중 예외 발생: {e}")
            return UploadWatchlistTaskOutput(
                date_str=date_str,
                status='error',
                message=f'저장 실패: {e}'
            )

        # 5. 성공 결과 반환
        return UploadWatchlistTaskOutput(
            date_str=date_str,
            status='success',
            message=f'{destination_name}에 HTS 형식으로 저장 완료'
        )