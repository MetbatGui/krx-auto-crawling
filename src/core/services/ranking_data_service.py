"""순위표 데이터 분석 서비스"""

import pandas as pd
from typing import Dict, Set, List


class RankingDataService:
    """순위표 관련 비즈니스 로직을 담당하는 서비스.

    외국인과 기관의 공통 매수 종목 계산 등 순수 비즈니스 로직만 포함합니다.
    Excel 등 기술 구현 세부사항에 의존하지 않습니다.
    """
    
    def __init__(self, top_n: int = 30):
        """RankingDataService 초기화.

        Args:
            top_n: 상위 몇 개 종목을 분석할지 (기본 20개)
        """
        self.top_n = top_n
    
    def calculate_common_stocks(
        self, 
        data_map: Dict[str, pd.DataFrame]
    ) -> Dict[str, Set[str]]:
        """시장별 외국인/기관 공통 매수 종목을 계산합니다.
        
        각 시장(KOSPI, KOSDAQ)에서 외국인과 기관이 모두 매수한 종목의
        교집합을 찾아 반환합니다.
        
        Args:
            data_map (Dict[str, pd.DataFrame]): 시장별 데이터 딕셔너리.
                예) {'KOSPI_foreigner': df1, 'KOSPI_institutions': df2, ...}
            
        Returns:
            Dict[str, Set[str]]: 시장별 공통 종목 Set.
                예) {'KOSPI': {'삼성전자', 'SK하이닉스'}, 'KOSDAQ': {...}}
        """
        return {
            market: self._calculate_market_common_stocks(market, data_map)
            for market in ['KOSPI', 'KOSDAQ']
        }
    
    def _calculate_market_common_stocks(
        self,
        market: str,
        data_map: Dict[str, pd.DataFrame]
    ) -> Set[str]:
        """특정 시장의 공통 종목을 계산합니다."""
        foreigner_df = data_map.get(f"{market}_foreigner")
        institutions_df = data_map.get(f"{market}_institutions")
        
        if foreigner_df is None or institutions_df is None:
            print(f"    -> [DataService:Ranking] {market} 데이터 부족")
            return set()
        
        top_foreigner = set(foreigner_df.head(self.top_n)['종목명'])
        top_institutions = set(institutions_df.head(self.top_n)['종목명'])
        
        common = top_foreigner & top_institutions
        print(f"    -> [DataService:Ranking] {market} 공통 종목 ({len(common)}개): {common}")
        
        return common
    
    def validate_data(self, data_list: List) -> bool:
        """데이터 유효성을 검증합니다.
        
        Args:
            data_list (List): 검증할 데이터 리스트.
            
        Returns:
            bool: 유효성 검증 결과.
        """
        if not data_list:
            print("[DataService:Ranking] ⚠️ 데이터가 없습니다")
            return False
        return True
