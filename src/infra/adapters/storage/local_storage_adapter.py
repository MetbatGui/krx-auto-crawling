"""
로컬 파일 시스템 저장소 구현

StoragePort를 구현하여 로컬 파일 시스템에 데이터를 저장합니다.
"""
import os
from pathlib import Path
from typing import Optional
import pandas as pd
import openpyxl

from core.ports.storage_port import StoragePort


class LocalStorageAdapter(StoragePort):
    """로컬 파일 시스템 저장소 Adapter.

    StoragePort를 구현하여 로컬 파일 시스템에 데이터를 저장합니다.

    Attributes:
        base_path (Path): 기본 저장 경로
    """
    
    def __init__(self, base_path: str = "output"):
        """LocalStorageAdapter 초기화.

        Args:
            base_path: 기본 저장 경로 (기본값: "output")
        """
        self.base_path = Path(base_path)
        self.ensure_directory("")  # 기본 경로 생성
        print(f"[LocalStorage] 초기화 완료 (Base: {self.base_path.absolute()})")
    
    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 Excel 파일로 저장합니다.

        Args:
            df: 저장할 DataFrame
            path: 저장 경로 (base_path 상대 경로)
            **kwargs: to_excel 옵션

        Returns:
            성공 여부
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            df.to_excel(full_path, **kwargs)
            print(f"[LocalStorage] ✅ Excel 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] 🚨 Excel 저장 실패 ({path}): {e}")
            return False
    
    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 CSV 파일로 저장합니다.

        Args:
            df: 저장할 DataFrame
            path: 저장 경로 (base_path 상대 경로)
            **kwargs: to_csv 옵션

        Returns:
            성공 여부
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            df.to_csv(full_path, **kwargs)
            print(f"[LocalStorage] ✅ CSV 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] 🚨 CSV 저장 실패 ({path}): {e}")
            return False
    
    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        """openpyxl Workbook을 저장합니다.

        Args:
            book: 저장할 Workbook 객체
            path: 저장 경로 (base_path 상대 경로)

        Returns:
            성공 여부
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            book.save(full_path)
            print(f"[LocalStorage] ✅ Workbook 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] 🚨 Workbook 저장 실패 ({path}): {e}")
            return False
    
    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        """Excel Workbook을 로드합니다.

        Args:
            path: 로드할 파일 경로 (base_path 상대 경로)

        Returns:
            openpyxl.Workbook: 로드된 Workbook 객체, 실패 시 None
        """
        try:
            full_path = self.base_path / path
            return openpyxl.load_workbook(full_path)
        except FileNotFoundError:
            print(f"[LocalStorage] ⚠️ 파일 없음: {path}")
            return None
        except Exception as e:
            print(f"[LocalStorage] 🚨 Workbook 로드 실패 ({path}): {e}")
            return None
    
    def path_exists(self, path: str) -> bool:
        """경로가 존재하는지 확인합니다.

        Args:
            path: 확인할 경로 (base_path 상대 경로)

        Returns:
            존재 여부
        """
        full_path = self.base_path / path
        return full_path.exists()
    
    def ensure_directory(self, path: str) -> bool:
        """디렉토리가 없으면 생성합니다.

        Args:
            path: 생성할 디렉토리 경로 (base_path 상대 경로)

        Returns:
            성공 여부
        """
        try:
            if path == "":
                # 기본 경로 생성
                self.base_path.mkdir(parents=True, exist_ok=True)
            else:
                full_path = self.base_path / path
                full_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            print(f"[LocalStorage] 🚨 디렉토리 생성 실패 ({path}): {e}")
            return False
