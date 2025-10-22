import pandas as pd
from typing import Dict, Any, List

# Core Interfaces & Dependencies
from core.tasks.base_task import Task
from src.crawler.daily_net_value_crawler import DailyNetValueCrawler

# --- 1. Crawl Task: 단일 시장/투자자 크롤링 (T1 & T2의 기본 단위) ---

class KrxFourWayCrawlTask(Task):
    """
    4가지 시장/투자자 조합에 대해 크롤링을 모두 실행하고 그 결과를 하나의 딕셔너리로 집계합니다.
    DailyNetValueCrawler 인스턴스를 재사용합니다.
    """
    def __init__(self, target_date: str):
        super().__init__()
        self.target_date = target_date
        # 크롤러 인스턴스를 Task 초기화 시점에 한 번만 생성합니다.
        self.crawler = DailyNetValueCrawler()
        
        # 새로운 구조를 위해 결과 키를 미리 정의합니다.
        self.RESULT_KEYS = [
            "KOSPI_foreigner", "KOSPI_institutions", 
            "KOSDAQ_foreigner", "KOSDAQ_institutions"
        ]

    def execute(self, input_data: Any = None) -> Dict[str, Any]:
        """
        4가지 크롤링 Task를 순차 실행하고 결과를 Dictionary 형태로 집계합니다.
        """
        results: Dict[str, Any] = {}
        
        MARKETS = ["KOSPI", "KOSDAQ"]
        INVESTORS = ["foreigner", "institutions"]
        
        print("\n[--- Starting 4-Way Crawl & Aggregate Task ---]")
        
        all_success = True

        # 4가지 조합에 대해 크롤러의 crawl() 메서드를 재사용하며 실행합니다.
        for market in MARKETS:
            for investor in INVESTORS:
                # 결과 딕셔너리의 직접적인 키 (예: 'KOSPI_foreigner')
                key = f"{market}_{investor}"
                
                try:
                    # 크롤러의 crawl() 메서드로 필요한 인자를 전달합니다.
                    df = self.crawler.crawl(
                        market=market,
                        investor=investor,
                        date_str=self.target_date
                    )
                    
                    results[key] = {
                        'status': 'SUCCESS',
                        'market': market,
                        'investor': investor,
                        'date': self.target_date,
                        'dataframe': df
                    }
                    status_icon = "✅"
                
                except Exception as e:
                    # 실패 처리: 에러 정보를 담아 집계하고, 플래그 설정
                    all_success = False
                    results[key] = {
                        'status': 'FAILED',
                        'market': market,
                        'investor': investor,
                        'date': self.target_date,
                        'error': str(e),
                        'dataframe': pd.DataFrame() 
                    }
                    status_icon = "❌"
                    
                print(f"   {status_icon} {key} Crawl Status: {results[key]['status']}")


        print("[--- Crawl & Aggregate Task Complete ---]")
        
        # 집계된 결과를 다음 Task로 전달. 최상위 딕셔너리에 상태와 날짜를 포함합니다.
        final_output = {
            'status': 'SUCCESS' if all_success else 'PARTIAL_FAILURE', 
            'date': self.target_date,
        }
        final_output.update(results) # 4개의 결과 필드를 최상위에 추가
        
        return final_output