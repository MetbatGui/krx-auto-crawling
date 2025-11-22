# infra/adapters/watchlist_file_adapter.py
import pandas as pd
from pathlib import Path
import os
from typing import List

from core.ports.watchlist_port import WatchlistPort
from core.domain.models import KrxData

class WatchlistFileAdapter(WatchlistPort):
    """
    WatchlistPort의 로컬 파일 시스템 구현체(Adapter)입니다.
    수집된 데이터에서 종목코드만 추출하여 HTS 업로드용 CSV 파일로 저장합니다.
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path) / 'watchlist'
        try:
            os.makedirs(self.base_path, exist_ok=True)
            print(f"[Adapter:WatchlistFile] 로컬 스토리지 초기화됨 (Base: {self.base_path})")
        except OSError as e:
            print(f"[Adapter:WatchlistFile] 🚨 기본 경로 생성 실패: {e}")
            raise

    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """
        수집된 데이터를 HTS 업로드용 CSV 파일로 저장합니다.
        (모든 데이터의 종목코드를 중복 제거하여 하나로 합침)
        """
        if not data_list:
            print("  [Adapter:WatchlistFile] ⚠️ 데이터가 없어 관심종목 저장을 건너뜁니다.")
            return

        # 1. 모든 데이터에서 종목코드 수집
        all_codes = set()
        date_str = data_list[0].date_str # 파일명용 날짜 (첫 번째 데이터 기준)

        for item in data_list:
            if not item.data.empty and '종목코드' in item.data.columns:
                all_codes.update(item.data['종목코드'].unique())

        if not all_codes:
            print("  [Adapter:WatchlistFile] ⚠️ 저장할 종목코드가 없습니다.")
            return

        # 2. DataFrame 생성 (헤더 포함, 종목코드 컬럼만)
        # HTS 등록용 포맷: 헤더 있음, 인덱스 없음, cp949 인코딩
        df_watchlist = pd.DataFrame({'종목코드': list(all_codes)})
        
        # 파일명: YYYYMMDD_watchlist.csv
        filename = f"{date_str}_watchlist.csv"
        full_path = self.base_path / filename
        
        try:
            df_watchlist.to_csv(
                full_path, 
                header=True,  
                index=False,    
                encoding='cp949'
            )
            print(f"  [Adapter:WatchlistFile] ✅ 관심종목 파일 저장 완료: {filename} ({len(df_watchlist)}개 종목)")
        except (IOError, OSError) as e:
            print(f"  [Adapter:WatchlistFile] 🚨 파일 저장 실패: {e}")