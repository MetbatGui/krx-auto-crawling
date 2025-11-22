"""
순위표 분석 비즈니스 로직 서비스

RankingExcelAdapter에서 분리된 공통 종목 계산 로직을 제공합니다.
"""
import pandas as pd
from typing import Dict, Set


class RankingAnalysisService:
    """순위표 분석 서비스"""
    
    TOP_N = 20  # 상위 몇 개 종목을 분석할지
    
    @staticmethod
    def calculate_common_stocks(
        data_map: Dict[str, pd.DataFrame]
    ) -> Dict[str, Set[str]]:
        """
        시장별 외국인/기관 공통 매수 종목을 계산합니다.
        
        Args:
            data_map: {key: DataFrame} 형태의 데이터
                예: {'KOSPI_foreigner': df1, 'KOSPI_institutions': df2, ...}
        
        Returns:
            시장별 공통 종목 Set
            예: {'KOSPI': {'삼성전자', 'SK하이닉스'}, 'KOSDAQ': {...}}
        """
        common_stocks = {}
        markets = ['KOSPI', 'KOSDAQ']
        
        for market in markets:
            foreigner_key = f"{market}_foreigner"
            inst_key = f"{market}_institutions"
            
            df_foreign = data_map.get(foreigner_key)
            df_inst = data_map.get(inst_key)
            
            if df_foreign is not None and df_inst is not None:
                # 상위 N개 종목 추출
                top_foreign = set(df_foreign.head(RankingAnalysisService.TOP_N)['종목명'])
                top_inst = set(df_inst.head(RankingAnalysisService.TOP_N)['종목명'])
                
                # 교집합 계산
                common = top_foreign.intersection(top_inst)
                common_stocks[market] = common
                
                print(f"    -> [Service:RankingAnalysis] {market} 공통 종목 ({len(common)}개): {common}")
            else:
                common_stocks[market] = set()
                print(f"    -> [Service:RankingAnalysis] {market} 데이터 부족, 공통 종목 없음")
        
        return common_stocks
