import itertools
from typing import Any, Dict, List, Optional, TypedDict

from core.tasks.base_task import Task
from core.ports.krx_data_port import KrxDataPort

class FetchKrxNetValueTaskInput(TypedDict):
    """FetchKrxNetValueTask execute 메서드의 입력을 정의합니다."""
    date_str: Optional[str]

class FetchKrxNetValueTaskOutput(TypedDict):
    """FetchKrxNetValueTask execute 메서드의 반환을 정의합니다."""
    date_str: Optional[str]
    status: str
    raw_bytes_dict: Optional[Dict[str, bytes]]
    message: Optional[str]

class FetchKrxNetValueTask(Task):
    """
    'KrxDataPort'를 사용하여 4가지 조합의 원본 데이터(bytes)를 수집합니다.
    I/O(데이터 수집) 책임만 갖습니다.
    """
    
    def __init__(self, krx_port: KrxDataPort):
        """FetchKrxNetValueTask를 초기화합니다.

        Args:
            krx_port (KrxDataPort): 파이프라인에서 주입된,
                KrxDataPort 인터페이스의 실제 구현체(Adapter).
        """
        self.krx_port = krx_port
        
        markets = ['KOSPI', 'KOSDAQ']
        investors = ['institutions', 'foreigner']
        self.targets: List[tuple[str, str]] = list(itertools.product(markets, investors))

    def execute(self, context: FetchKrxNetValueTaskInput) -> FetchKrxNetValueTaskOutput:
        """Task의 핵심 로직(I/O)을 실행합니다.

        `self.targets`에 정의된 4가지 조합(시장/투자자)에 대해
        `KrxDataPort`를 호출하여 원본 엑셀(bytes)을 수집합니다.

        Args:
            context (FetchKrxNetValueTaskInput): 파이프라인의 공유 컨텍스트.
                `date_str` (Optional[str]) 키를 포함합니다.

        Returns:
            FetchKrxNetValueTaskOutput: Task의 실행 결과를 담은 TypedDict.
                - 'status': 'success', 'partial_success', 'error' 중 하나.
                - 'raw_bytes_dict': 수집 성공한 데이터 (key: 'MARKET_INVESTOR').
                - 'message': 실행 결과 요약 메시지.
        """
        print(f"--- [Task] {self.__class__.__name__} 시작 (I/O) ---")

        date_str: Optional[str] = context.get('date_str')
        raw_bytes_dict: Dict[str, bytes] = {}
        failed_targets: List[str] = []
        
        for market, investor in self.targets:
            key = f"{market}_{investor}"
            try:
                print(f"  -> {key} 원본 데이터 수집 요청 (Port 호출)")
                raw_data = self.krx_port.fetch_net_value_data(market, investor, date_str)
                raw_bytes_dict[key] = raw_data
                
            except Exception as e:
                print(f"  -> 🚨 {key} 수집 실패: {e}")
                failed_targets.append(key)
        
        if not raw_bytes_dict:
            return FetchKrxNetValueTaskOutput(
                date_str=date_str,
                status='error',
                raw_bytes_dict=None,
                message='모든 원본 데이터 수집 실패'
            )
            
        if failed_targets:
            message = f"부분 성공. (실패: {', '.join(failed_targets)})"
            status = 'partial_success'
        else:
            message = '모든 원본 데이터 수집 완료'
            status = 'success'
            
        return FetchKrxNetValueTaskOutput(
            date_str=date_str,
            status=status,
            raw_bytes_dict=raw_bytes_dict,
            message=message
        )