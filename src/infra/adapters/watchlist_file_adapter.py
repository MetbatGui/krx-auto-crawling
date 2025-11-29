"""Watchlist 파일 저장 어댑터"""

import pandas as pd
from typing import List, Dict

from core.ports.watchlist_port import WatchlistPort
from core.ports.storage_port import StoragePort
from core.domain.models import KrxData


class WatchlistFileAdapter(WatchlistPort):
    """WatchlistPort 구현체.

    일별/누적 상위 종목을 HTS 업로드용 CSV 파일로 저장합니다.

    Attributes:
        storages (List[StoragePort]): 파일 저장 포트 리스트
    """
    
    REPORT_ORDER = ['KOSPI_foreigner', 'KOSDAQ_foreigner', 'KOSPI_institutions', 'KOSDAQ_institutions']
    TOP_N = 20
    
    def __init__(self, storages: List[StoragePort]):
        """WatchlistFileAdapter 초기화.

        Args:
            storages: StoragePort 구현체 리스트
        """
        self.storages = storages
        for storage in self.storages:
            storage.ensure_directory("watchlist")
        print(f"[Adapter:WatchlistFile] 초기화 완료 (저장소 {len(self.storages)}개)")

    def save_watchlist(self, data_list: List[KrxData]) -> None:
        """일별 상위 종목을 CSV 파일로 저장합니다.
        
        각 리포트별 상위 20개씩, 총 80개 종목을 저장합니다.
        순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        
        Args:
            data_list: KRX 데이터 리스트
        """
        if not data_list:
            print("  [Adapter:WatchlistFile] ⚠️ 데이터가 없어 저장을 건너뜁니다")
            return

        date_str = data_list[0].date_str
        
        # 각 리포트별 상위 20개 종목명 추출
        top_stocks_map = {}
        for item in data_list:
            if item.data.empty or '종목명' not in item.data.columns:
                continue
            top_stocks_map[item.key] = item.data['종목명'].head(self.TOP_N).tolist()
        
        if not top_stocks_map:
            print("  [Adapter:WatchlistFile] ⚠️ 저장할 종목이 없습니다")
            return
        
        # 공통 저장 로직 사용
        self._save_stock_list(
            top_stocks_map,
            date_str,
            f"{date_str}_일별상위종목.csv",
            "일별 상위종목"
        )
    
    def save_cumulative_watchlist(self, top_stocks: Dict[str, List[str]], date_str: str) -> None:
        """누적 상위 종목을 CSV 파일로 저장합니다.
        
        마스터 리포트의 총계 기준 상위 20개씩, 총 80개 종목을 저장합니다.
        순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        
        Args:
            top_stocks: 리포트별 상위 종목 딕셔너리
            date_str: 날짜 문자열
        """
        if not top_stocks:
            print("  [Adapter:WatchlistFile] ⚠️ 누적 상위종목 데이터가 없어 저장을 건너뜁니다")
            return
        
        # 공통 저장 로직 사용
        self._save_stock_list(
            top_stocks,
            date_str,
            f"{date_str}_누적상위종목.csv",
            "누적 상위종목"
        )
    
    def _save_stock_list(
        self,
        top_stocks: Dict[str, List[str]],
        date_str: str,
        filename: str,
        description: str
    ) -> None:
        """종목 리스트를 CSV 파일로 저장하는 공통 로직.
        
        Args:
            top_stocks: 리포트별 종목 딕셔너리
            date_str: 날짜 문자열
            filename: 저장할 파일명
            description: 로그용 설명
        """
        # 정해진 순서대로 종목 수집 (80개)
        # 순서: KOSPI외국인 → KOSDAQ외국인 → KOSPI기관 → KOSDAQ기관
        all_stock_names = []
        for key in self.REPORT_ORDER:
            if key in top_stocks:
                all_stock_names.extend(top_stocks[key])
        
        if not all_stock_names:
            print(f"  [Adapter:WatchlistFile] ⚠️ 저장할 {description}이 없습니다")
            return
        
        # DataFrame 생성 (헤더: 종목명)
        df = pd.DataFrame({'종목명': all_stock_names})
        
        # 👇 추가: 각 행 끝에 쉼표(,)를 붙이기 위해 빈 문자열의 '쉼표' 열을 추가합니다.
        # 이렇게 하면 CSV 저장 시 '종목명,쉼표' 형태가 되고, '종목명,,' 형태로 저장되어 
        # HTS 포맷 요구사항(종목명 다음에 데이터 없는 필드)을 만족하거나,
        # '종목명' 열만 사용하고 나머지 빈 필드를 위해 쉼표를 명시적으로 추가하는 효과를 낼 수 있습니다.
        # HTS 포맷에 따라 '쉼표' 열의 이름을 빈 문자열로 설정할 수도 있습니다.
        df[''] = '' # 빈 헤더의 열을 추가하여 CSV에 추가 쉼표를 생성
        
        # 저장
        file_path = f"watchlist/{filename}"
        
        for storage in self.storages:
            success = storage.save_dataframe_csv(
                df,
                path=file_path,
                # '종목명'과 빈 헤더를 모두 저장하기 위해 header=True 유지
                header=True,
                index=False,
                encoding='cp949'
            )
            
            if success:
                storage_name = storage.__class__.__name__
                print(f"  [Adapter:WatchlistFile] ✅ {storage_name} {description} 파일 저장 완료: {filename} ({len(df)}개 종목)")