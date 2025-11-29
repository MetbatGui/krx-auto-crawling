"""
데이터 저장을 위한 포트 인터페이스

저장 위치(로컬, 클라우드)와 무관하게 데이터를 저장하고 로드할 수 있도록 추상화합니다.
"""
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
import openpyxl


class StoragePort(ABC):
    """
    데이터 저장을 위한 포트 인터페이스.
    
    이 인터페이스를 구현하여 다양한 저장소(로컬, Google Drive, S3 등)를 
    지원할 수 있습니다.
    """
    
    @abstractmethod
    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """
        DataFrame을 Excel 파일로 저장합니다.
        
        Args:
            df: 저장할 DataFrame
            path: 저장 경로 (상대 경로)
            **kwargs: pandas.to_excel()에 전달할 추가 인자
            
        Returns:
            성공 여부
        """
        pass
    
    @abstractmethod
    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """
        DataFrame을 CSV 파일로 저장합니다.
        
        Args:
            df: 저장할 DataFrame
            path: 저장 경로 (상대 경로)
            **kwargs: pandas.to_csv()에 전달할 추가 인자
            
        Returns:
            성공 여부
        """
        pass
    
    @abstractmethod
    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        """
        openpyxl Workbook을 저장합니다.
        
        Args:
            book: 저장할 Workbook
            path: 저장 경로 (상대 경로)
            
        Returns:
            성공 여부
        """
        pass
    
    @abstractmethod
    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        """
        Excel Workbook을 로드합니다.
        
        Args:
            path: 로드할 파일 경로 (상대 경로)
            
        Returns:
            Workbook 객체 또는 None (파일이 없는 경우)
        """
        pass
    
    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """
        경로가 존재하는지 확인합니다.
        
        Args:
            path: 확인할 경로 (상대 경로)
            
        Returns:
            존재 여부
        """
        pass
    
    @abstractmethod
    def ensure_directory(self, path: str) -> bool:
        """
        디렉토리가 없으면 생성합니다.
        
        Args:
            path: 디렉토리 경로 (상대 경로)
            
        Returns:
            성공 여부
        """
        pass

    @abstractmethod
    def load_dataframe(self, path: str, sheet_name: str = None, **kwargs) -> pd.DataFrame:
        """
        Excel 파일에서 DataFrame을 로드합니다.

        Args:
            path: 파일 경로 (상대 경로)
            sheet_name: 시트 이름 (None이면 첫 번째 시트)
            **kwargs: pandas.read_excel()에 전달할 추가 인자

        Returns:
            로드된 DataFrame (실패 시 빈 DataFrame 반환 권장)
        """
        pass

    @abstractmethod
    def get_file(self, path: str) -> Optional[bytes]:
        """
        파일의 내용을 바이트로 읽어옵니다.
        
        Args:
            path: 파일 경로 (상대 경로)
            
        Returns:
            파일 내용 (bytes) 또는 None (파일이 없는 경우)
        """
        pass

    @abstractmethod
    def put_file(self, path: str, data: bytes) -> bool:
        """
        바이트 데이터를 파일로 저장합니다.
        
        Args:
            path: 저장 경로 (상대 경로)
            data: 저장할 데이터 (bytes)
            
        Returns:
            성공 여부
        """
        pass
