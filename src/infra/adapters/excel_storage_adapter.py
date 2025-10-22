import pandas as pd
from pathlib import Path
import os

from core.ports.storage_port import StoragePort

class ExcelStorageAdapter(StoragePort):
    """
    StoragePort의 '엑셀(XLSX)' 구현체입니다.
    DataFrame을 지정된 경로에 .xlsx 파일로 저장합니다. (헤더 포함)
    """
    
    def __init__(self, base_path: str):
        """ExcelStorageAdapter를 초기화합니다.

        Args:
            base_path (str): 엑셀 파일이 저장될 기본 디렉터리 경로.
                                (예: 'output')
        """
        self.base_path = Path(base_path) / '순매수'
        try:
            os.makedirs(self.base_path, exist_ok=True)
            print(f"[Adapter:Excel] 엑셀 스토리지 초기화됨 (Base: {self.base_path})")
        except OSError as e:
            print(f"[Adapter:Excel] 🚨 기본 경로 생성 실패: {e}")
            raise

    def save(self, df: pd.DataFrame, destination_name: str) -> bool:
        """
        DataFrame을 .xlsx 파일로 저장합니다. (헤더 포함, 인덱스 제외)

        Args:
            df (pd.DataFrame): 저장할 DataFrame.
            destination_name (str): '20251021_외국인코스피.xlsx'와 같은 파일 이름.

        Returns:
            bool: 저장 성공 시 True, 실패 시 False.
        """
        
        full_path = self.base_path / destination_name
        
        try:
            df.to_excel(
                full_path, 
                header=True,   # <-- 엑셀이므로 헤더 포함 (기본값)
                index=False    # <-- 인덱스는 제외
            )
            print(f"  [Adapter:Excel] '{full_path}' 파일로 DF 저장 (행: {len(df)})")
            return True
        except (IOError, OSError) as e:
            print(f"  [Adapter:Excel] 🚨 파일 저장 실패: {e}")
            return False