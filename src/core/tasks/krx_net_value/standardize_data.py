from typing import Any, Dict, List, Optional, TypedDict
import pandas as pd

from core.tasks.base_task import Task
from core.components.krx_processor import process_krx_net_value_excel
# 이전 Task의 Output이 이 Task의 Input이 됩니다.
from core.tasks.krx_net_value.fetch_raw_data import FetchKrxNetValueTaskOutput

# 1. 입력 TypedDict 정의
class StandardizeDataTaskInput(FetchKrxNetValueTaskOutput):
    """
    StandardizeKrxDataTask execute 메서드의 입력을 정의합니다.
    (Fetch Task의 Output을 그대로 받습니다)
    """
    pass

# 2. 출력 TypedDict 정의
class StandardizeDataTaskOutput(TypedDict):
    """StandardizeKrxDataTask execute 메서드의 반환을 정의합니다."""
    date_str: Optional[str]
    status: str
    processed_dfs_dict: Optional[Dict[str, pd.DataFrame]]
    message: Optional[str]


class StandardizeKrxDataTask(Task):
    """
    Fetch Task가 수집한 'raw_bytes_dict'를 입력받아,
    'krx_processor' 컴포넌트를 사용해 표준화된 DataFrame 딕셔너리로 변환합니다.
    (순수 가공 책임)
    """

    def __init__(self):
        """StandardizeKrxDataTask를 초기화합니다.
        (이 Task는 Port에 의존하지 않으므로 주입받을 인자가 없습니다.)
        """
        pass

    def execute(self, context: StandardizeDataTaskInput) -> StandardizeDataTaskOutput:
        """Task의 핵심 로직(가공/표준화)을 실행합니다.

        Args:
            context (StandardizeDataTaskInput): 이전 Fetch Task의 결과.
                `raw_bytes_dict` (Dict[str, bytes]) 키를 포함합니다.

        Returns:
            StandardizeDataTaskOutput: Task의 실행 결과를 담은 TypedDict.
                - 'processed_dfs_dict': 가공/표준화된 DataFrame 딕셔너리.
        """
        print(f"--- [Task] {self.__class__.__name__} 시작 (Standardize) ---")

        date_str = context.get('date_str')
        raw_bytes_dict = context.get('raw_bytes_dict')

        # 1. 이전 Task의 결과(bytes) 유효성 검사
        if context.get('status') == 'error' or not raw_bytes_dict:
            print("  -> 🚨 이전 Task(Fetch)가 실패했거나 원본 데이터가 없습니다.")
            return StandardizeDataTaskOutput(
                date_str=date_str,
                status='skipped',
                processed_dfs_dict=None,
                message='이전 Task 실패로 건너뜀'
            )

        processed_dfs_dict: Dict[str, pd.DataFrame] = {}
        failed_keys: List[str] = []

        # 2. 각 raw_bytes를 순회하며 가공 (Component 호출)
        for key, raw_bytes in raw_bytes_dict.items():
            if not raw_bytes:
                print(f"  -> ⚠️ {key} 원본 데이터(bytes)가 없어 건너뜁니다.")
                failed_keys.append(key)
                continue
            
            try:
                # Component(순수 로직)로 데이터 가공
                df = process_krx_net_value_excel(raw_bytes)
                
                # --- [수정된 부분] ---
                # Upload Task의 책임을 Standardize Task로 이동
                # '종목코드' 컬럼이 있다면 표준화 단계에서 미리 제거
                if '종목코드' in df.columns:
                    df = df.drop(columns=['종목코드'])
                # ---------------------

                if not df.empty:
                    processed_dfs_dict[key] = df
                else:
                    # 컴포넌트가 빈 DF를 반환 (휴장일 등)
                    print(f"  -> ⚠️ {key} 가공 결과가 비어있습니다.")
                    failed_keys.append(key)

            except Exception as e:
                print(f"  -> 🚨 {key} 가공 중 예외 발생: {e}")
                failed_keys.append(key)

        # 3. 결과 반환
        if not processed_dfs_dict:
            return StandardizeDataTaskOutput(
                date_str=date_str,
                status='error',
                processed_dfs_dict=None,
                message='모든 데이터 표준화/가공 실패'
            )

        if failed_keys:
            message = f"부분 성공. (가공 실패/제외: {', '.join(failed_keys)})"
            status = 'partial_success'
        else:
            message = '모든 데이터 표준화 완료'
            status = 'success'

        return StandardizeDataTaskOutput(
            date_str=date_str,
            status=status,
            processed_dfs_dict=processed_dfs_dict,
            message=message
        )