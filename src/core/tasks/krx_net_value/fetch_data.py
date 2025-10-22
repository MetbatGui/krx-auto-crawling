from typing import Any, Dict, List
import pandas as pd

from core.tasks.base_task import Task
from core.ports.krx_data_port import KrxDataPort
from core.components.krx_processor import process_krx_net_value_excel

class KRXNetValueFetchDataTask(Task):
    """
    'KrxDataPort'를 사용하여 4가지 시장/투자자 조합의 데이터를 수집하고,
    'krx_processor'를 사용하여 가공(상위 20개 추출)하는 Task.
    """
    
    def __init__(self, krx_port: KrxDataPort):
        """
        [의존성 주입]
        이 Task는 'KrxDataPort'라는 '약속(Port)'에만 의존합니다.
        
        Args:
            krx_port (KrxDataPort): 파이프라인에서 주입해주는 
                                    KrxDataPort의 실제 구현체(Adapter).
        """
        self.krx_port = krx_port
        self.targets = [
            ('KOSPI', 'institutions'),
            ('KOSPI', 'foreigner'),
            ('KOSDAQ', 'institutions'),
            ('KOSDAQ', 'foreigner'),
        ]

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Task 실행: 4회 수집 및 가공
        """
        print(f"--- [Task] {self.__class__.__name__} 시작 ---")
        
        processed_dfs: List[pd.DataFrame] = []

        for market, investor in self.targets:
            try:
                # 1. Port(약속)를 통해 데이터 수집 (I/O)
                # Task는 Adapter(KrxHttpAdapter)의 존재를 모릅니다.
                print(f"  -> {market} {investor} 데이터 수집 요청 (Port 호출)")
                raw_data_bytes = self.krx_port.fetch_net_value_data(market, investor)
                
                # 2. Component(순수 로직)로 데이터 가공
                df = process_krx_net_value_excel(raw_data_bytes)
                
                if not df.empty:
                    # 3. 메타데이터 추가 (다음 Task가 사용)
                    df.market = market
                    df.investor = investor
                    processed_dfs.append(df)
                else:
                    print(f"  -> ⚠️ {market} {investor} 가공 결과가 비어있습니다.")
            
            except Exception as e:
                # 특정 조합이 실패해도 파이프라인을 중단시키지 않고 계속합니다.
                print(f"  -> 🚨 {market} {investor} 처리 중 예외 발생: {e}")
        
        # 4. 결과 반환 -> Context에 병합됨
        if not processed_dfs:
            print(f"  -> [Task] 모든 데이터 수집/가공에 실패했습니다.")
            return {'status': 'error', 'message': '모든 KRX 데이터 수집/가공 실패'}
        
        print(f"  -> [Task] 총 {len(processed_dfs)}개 DF 가공 완료.")
        
        # 다음 Task(e.g., ProcessWatchlistTask)가 사용할 수 있도록
        # 'processed_dfs' 키로 가공된 DF 리스트를 컨텍스트에 추가합니다.
        return {
            'status': 'success',
            'processed_dfs': processed_dfs 
        }