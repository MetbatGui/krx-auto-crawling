import pandas as pd
from pathlib import Path
import os
from typing import Optional

from core.ports.storage_port import StoragePort

class LocalStorageAdapter(StoragePort):
    """
    StoragePort의 로컬 파일 시스템 구현체(Adapter)입니다.
    DataFrame을 지정된 기본 경로(base_path) 하위에 CSV 파일로 저장합니다.
    """
    
    def __init__(self, base_path: str):
        """LocalStorageAdapter를 초기화합니다.

        Args:
            base_path (str): 산출물 경로.
                             (예: 'output')
        """
        self.base_path = Path(base_path) / 'watchlist'
        try:
            # 'output/watchlist' 같은 중첩 디렉터리도 생성 (exist_ok=True)
            os.makedirs(self.base_path, exist_ok=True)
            print(f"[Adapter:Local] 로컬 스토리지 초기화됨 (Base: {self.base_path})")
        except OSError as e:
            print(f"[Adapter:Local] 🚨 기본 경로 생성 실패: {e}")
            raise

    def save(self, df: pd.DataFrame, destination_name: str) -> bool:
        """
        DataFrame을 CSV 파일로 저장합니다. (영웅문 HTS 형식: 헤더X, 인덱스X)

        Args:
            df (pd.DataFrame): 저장할 DataFrame.
            destination_name (str): '20251022_watchlist.csv'와 같은 파일 이름.

        Returns:
            bool: 저장 성공 시 True, 실패 시 False.
        """
        
        # (base_path / destination_name) 결합
        # 예: 'output/watchlist' / '20251022_watchlist.csv'
        full_path = self.base_path / destination_name
        
        try:
            df.to_csv(
                full_path, 
                header=False,  # <-- HTS 형식 (헤더 없음)
                index=False    # <-- HTS 형식 (인덱스 없음)
            )
            print(f"  [Adapter:Local] '{full_path}' 파일로 DF 저장 (행: {len(df)})")
            return True
        except (IOError, OSError) as e:
            print(f"  [Adapter:Local] 🚨 파일 저장 실패: {e}")
            return False