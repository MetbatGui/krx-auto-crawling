import pytest
import pandas as pd
import os
from src.infra.adapters.storage.local_storage_adapter import LocalStorageAdapter

def test_local_storage_save_and_load_dataframe(tmp_path):
    """LocalStorageAdapter가 실제 파일 시스템에 DataFrame을 저장하고 로드하는지 검증"""
    # Given
    # tmp_path는 pytest가 제공하는 임시 디렉토리 (테스트 후 자동 삭제됨)
    adapter = LocalStorageAdapter(base_path=str(tmp_path))
    
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    file_path = "test_folder/data.xlsx"
    
    # When
    # 1. 저장
    save_result = adapter.save_dataframe_excel(df, file_path, index=False)
    
    # Then
    assert save_result is True
    
    # 실제 파일이 생성되었는지 확인
    full_path = tmp_path / "test_folder" / "data.xlsx"
    assert full_path.exists()
    
    # 2. 로드
    loaded_df = adapter.load_dataframe(file_path)
    assert not loaded_df.empty
    assert len(loaded_df) == 2
    assert loaded_df['col1'].iloc[0] == 1

def test_local_storage_path_exists(tmp_path):
    """파일 존재 여부 확인 기능 검증"""
    # Given
    adapter = LocalStorageAdapter(base_path=str(tmp_path))
    file_path = "check_exists.txt"
    full_path = tmp_path / file_path
    
    # 파일 생성
    full_path.write_text("content", encoding="utf-8")
    
    # When & Then
    assert adapter.path_exists(file_path) is True
    assert adapter.path_exists("non_existent_file.txt") is False

def test_local_storage_put_and_get_file(tmp_path):
    """바이트 데이터 저장 및 로드 검증"""
    # Given
    adapter = LocalStorageAdapter(base_path=str(tmp_path))
    file_path = "binary.dat"
    data = b"\x00\x01\x02"
    
    # When
    adapter.put_file(file_path, data)
    
    # Then
    loaded_data = adapter.get_file(file_path)
    assert loaded_data == data

def test_local_storage_ensure_directory(tmp_path):
    """디렉토리 생성 검증"""
    # Given
    adapter = LocalStorageAdapter(base_path=str(tmp_path))
    dir_path = "deep/nested/dir"
    
    # When
    adapter.ensure_directory(dir_path)
    
    # Then
    full_path = tmp_path / "deep" / "nested" / "dir"
    assert full_path.exists()
    assert full_path.is_dir()
