
import sys
import os
import pandas as pd
import openpyxl
from io import BytesIO
from typing import Optional

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData, Market, Investor

sys.stdout.reconfigure(encoding='utf-8')

class MockStorage(StoragePort):
    def __init__(self):
        self.files = {} # Stores pandas DataFrame for simplicity in this test
        self.raw_files = {} # Stores bytes

    def save_dataframe_excel(self, df: pd.DataFrame, path: str, index: bool = False, **kwargs) -> bool:
        self.files[path] = df
        print(f"[MockStorage] Saved Excel: {path}")
        return True

    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        self.files[path] = df
        print(f"[MockStorage] Saved CSV: {path}")
        return True

    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        virtual_wb = BytesIO()
        book.save(virtual_wb)
        self.raw_files[path] = virtual_wb.getvalue()
        print(f"[MockStorage] Saved Workbook: {path}")
        return True

    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        if path in self.raw_files:
            return openpyxl.load_workbook(BytesIO(self.raw_files[path]))
        return None

    def path_exists(self, path: str) -> bool:
        return path in self.files or path in self.raw_files

    def ensure_directory(self, path: str) -> bool:
        return True

    def load_dataframe(self, path: str, sheet_name: str = None, **kwargs) -> pd.DataFrame:
        print(f"[MockStorage] Loading DataFrame: {path}")
        if path in self.files:
            return self.files[path]
        return pd.DataFrame()

    def get_file(self, path: str) -> Optional[bytes]:
        return self.raw_files.get(path)

    def put_file(self, path: str, data: bytes) -> bool:
        self.raw_files[path] = data
        return True

def test_daily_adapter_load():
    # 1. Setup Mock Storage and Adapter
    mock_storage = MockStorage()
    adapter = DailyExcelAdapter(storages=[mock_storage], source_storage=mock_storage)
    
    # 2. Prepare Dummy Data
    date_str = "20251215"
    test_cases = [
        ('KOSPI_foreigner', Market.KOSPI, Investor.FOREIGNER),
        ('KOSPI_institutions', Market.KOSPI, Investor.INSTITUTIONS),
        ('KOSDAQ_foreigner', Market.KOSDAQ, Investor.FOREIGNER),
        ('KOSDAQ_institutions', Market.KOSDAQ, Investor.INSTITUTIONS),
    ]
    
    print("\n--- Saving Dummy Data ---")
    sr_data_list = []
    for key, market, investor in test_cases:
        df = pd.DataFrame({
            '종목명': ['TestStock'], 
            '거래대금_순매수': [1000]
        })

        krx_data = KrxData(market, investor, date_str, df)
        sr_data_list.append(krx_data)
        
    adapter.save_daily_reports(sr_data_list)
    
    # 3. Test Load
    print("\n--- Testing Load ---")
    loaded_data = adapter.load_daily_reports(date_str)
    
    if len(loaded_data) == 4:
        print("\nSUCCESS: All 4 reports loaded.")
        for item in loaded_data:
            print(f" - {item.market} {item.investor}: {len(item.data)} rows")
    else:
        print(f"\nFAILURE: Loaded {len(loaded_data)} reports. Expected 4.")
        exit(1)

if __name__ == "__main__":
    test_daily_adapter_load()
