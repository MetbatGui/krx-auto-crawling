from abc import ABC, abstractmethod
import pandas as pd

class StoragePort(ABC):
    """
    데이터(DataFrame)를 특정 저장소에 저장하기 위한 인터페이스(Port)입니다.
    """
    
    @abstractmethod
    def save(self, df: pd.DataFrame, destination_name: str) -> bool:
        """
        주어진 DataFrame을 지정된 이름으로 저장소에 저장합니다.

        Args:
            df (pd.DataFrame): 저장할 데이터.
            destination_name (str): 저장 위치를 식별하는 이름.
                (Adapter에 따라 시트 이름, 파일 경로, 메모리 키 등이 됩니다.)

        Returns:
            bool: 저장 성공 시 True, 실패 시 False.
        """
        pass