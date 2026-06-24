"""네이버 금융 fchart API 기반 가격 데이터 조회 어댑터"""

import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict

from core.ports.price_data_port import PriceDataPort, StockPriceInfo


class NaverPriceDataAdapter(PriceDataPort):
    """Naver fchart API를 활용하여 가격 정보를 조회하는 어댑터"""

    BASE_URL = "https://fchart.stock.naver.com/sise.nhn"

    def __init__(self, max_workers: int = 10):
        """NaverPriceDataAdapter 초기화.
        
        Args:
            max_workers (int): 병렬 처리 스레드 풀 크기.
        """
        self.max_workers = max_workers
        self.session = requests.Session()
        # 일반적으로 접속 거부를 방지하기 위해 UA 추가
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        })
        print(f"[Adapter:NaverPrice] 초기화 완료 (max_workers={self.max_workers})")

    def get_price_info(self, ticker: str, date_str: str) -> Optional[StockPriceInfo]:
        """단일 종목의 가격 정보를 네이버 fchart API를 통해 조회합니다."""
        url = f"{self.BASE_URL}?symbol={ticker}&timeframe=day&count=3650&requestType=0"
        
        try:
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
            
            # EUC-KR 디코딩 후 XML 선언부 제거 (ElementTree의 multi-byte encoding 에러 우회)
            xml_text = response.content.decode('euc-kr')
            import re
            xml_text = re.sub(r'<\?xml.*?\?>', '', xml_text).strip()
            
            root = ET.fromstring(xml_text)
            items = root.findall('.//item')
            if not items:
                print(f"  [NaverPrice] {ticker} 데이터 없음")
                return None
                
            historical_highs = []
            close_price = 0.0
            found_date = False
            
            for item in items:
                data = item.attrib.get('data')
                if not data:
                    continue
                    
                parts = data.split('|')
                if len(parts) >= 6:
                    item_date_str = parts[0]
                    close_val = float(parts[4])  # 종가(Close) 기준
                    
                    if item_date_str == date_str:
                        close_price = close_val
                        found_date = True
                    elif item_date_str < date_str:
                        historical_highs.append(close_val)  # 종가 기준으로 신고가 연산
                        last_close_price = close_val
            
            # 지정한 기준일(date_str)의 종가를 정확히 찾지 못한 경우
            # (예: 크롤링 당일 네이버 반영 지연 등) 마지막 과거 영업일의 종가를 임시로 사용
            if not found_date:
                if not historical_highs:
                    print(f"  [NaverPrice] {ticker} 기준일({date_str}) 및 과거 데이터 없음")
                    return None
                close_price = last_close_price
                print(f"  [NaverPrice] {ticker} 기준일({date_str}) 미발견, 최근 종가({close_price}) 사용")

            all_time_high = max(historical_highs) if historical_highs else close_price
            
            recent_250_days = historical_highs[-250:]
            high_52w = max(recent_250_days) if recent_250_days else all_time_high

            return StockPriceInfo(
                ticker=ticker,
                close_price=close_price,
                high_52w=high_52w,
                all_time_high=all_time_high
            )

        except Exception as e:
            print(f"  [NaverPrice] {ticker} 차트 조회 오류: {e}")
            return None

    def get_bulk_price_info(self, tickers: list[str], date_str: str) -> dict[str, StockPriceInfo]:
        """PriceDataPort 구현: 여러 종목의 가격 정보(신고가 포함)를 ThreadPool로 빠르게 반환합니다."""
        print(f"  [NaverPrice] {len(tickers)}개 종목 벌크 가격 조회 시작 ({date_str})")
        
        result: Dict[str, StockPriceInfo] = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self.get_price_info, ticker, date_str): ticker
                for ticker in tickers
            }
            
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    price_info = future.result()
                    if price_info:
                        result[ticker] = price_info
                except Exception as e:
                    print(f"  [NaverPrice] {ticker} 배치 작업 오류: {e}")
                    
        print(f"  [NaverPrice] 벌크 조회 완료: {len(result)}/{len(tickers)}개 성공")
        return result
