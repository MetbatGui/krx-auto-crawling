from typing import Dict, Optional, List
import pandas as pd
import openpyxl
from core.ports.storage_port import StoragePort

class FakeStorageAdapter(StoragePort):
    """테스트용 인메모리 스토리지 어댑터"""
    
    def __init__(self):
        self.files: Dict[str, bytes] = {}
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self.workbooks: Dict[str, openpyxl.Workbook] = {}
        self.directories: List[str] = []

    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        self.dataframes[path] = df.copy()
        return True

    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        self.dataframes[path] = df.copy()
        return True

    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        self.workbooks[path] = book
        return True

    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        return self.workbooks.get(path)

    def path_exists(self, path: str) -> bool:
        return (path in self.files) or (path in self.dataframes) or (path in self.workbooks)

    def ensure_directory(self, path: str) -> bool:
        self.directories.append(path)
        return True

    def load_dataframe(self, path: str, sheet_name: str = None, **kwargs) -> pd.DataFrame:
        return self.dataframes.get(path, pd.DataFrame())

    def get_file(self, path: str) -> Optional[bytes]:
        return self.files.get(path)

    def put_file(self, path: str, data: bytes) -> bool:
        self.files[path] = data
        return True
