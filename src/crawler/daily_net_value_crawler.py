import pandas as pd
import cloudscraper
import io
import datetime
import os
import time
from dotenv import load_dotenv

from crawler.crawler import Crawler

load_dotenv()

OTP_URL = os.getenv('KRX_OTP_URL')
DOWNLOAD_URL = os.getenv('KRX_DOWNLOAD_URL')

class DailyNetValueCrawler(Crawler):
    """KRX의 투자자별 일별 순매수 상위 20 종목을 크롤링합니다."""
    
    def __init__(self):
        """
        초기화 시점에 크롤링 대상 인자를 받지 않고, 공통 자원(scraper)만 준비합니다.
        """
        super().__init__()
        # Cloudscraper 인스턴스 생성 (Cloudflare 우회용)
        self.scraper = cloudscraper.create_scraper()
        
    def create_otp_params(self, market: str, investor: str, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 페이로드를 생성합니다."""
        
        market = market.upper()
        investor = investor.lower()
        
        params = {
            'locale': 'ko_KR',
            'invstTpCd': '',
            'strtDd': target_date,
            'endDd': target_date,
            'share': '1', # 주식수 기준
            'money': '3', # 거래대금 기준
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401' # 투자자별 매매종합
        }
        
        # 1. 시장 구분 (mktId)
        if market == 'KOSPI':
            params['mktId'] = 'STK'
        elif market == 'KOSDAQ':
            params['mktId'] = 'KSQ'
            params['segTpCd'] = 'ALL' 
        else:
            raise ValueError(f"Unsupported market ID: {market}")

        # 2. 투자자 구분 (invstTpCd)
        if investor == 'institutions':
            params['invstTpCd'] = '7050' # 기관
        elif investor == 'foreigner':
            params['invstTpCd'] = '9000' # 외국인
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
            
        return params
    
    def crawl(self, market: str, investor: str, date_str: str = None) -> pd.DataFrame:
        """
        OTP 발급 후 파일 다운로드 및 상위 20개 필터링을 수행합니다.
        :param market: 시장 구분 ('KOSPI', 'KOSDAQ')
        :param investor: 투자자 구분 ('institutions', 'foreigner')
        :param date_str: YYYYMMDD 형식의 날짜. 지정하지 않으면 오늘 날짜 사용.
        """
        
        # 1. 날짜 설정 및 파라미터 준비
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
            
        market = market.upper()
        investor = investor.lower()
            
        # KRX 요청 간격 유지를 위해 짧은 지연 시간 추가
        time.sleep(1) 
        
        otp_payload = self.create_otp_params(market, investor, target_date)
        
        print(f"   -> Crawling {market} ({investor}) for {target_date}")
        
        # --- 2단계: OTP 생성 요청 ---
        otp_response = self.scraper.post(OTP_URL, data=otp_payload, verify=True)
        otp_response.raise_for_status() 
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            raise ConnectionError(f"OTP acquisition failed. Check date or market status. Response snippet: {otp_code[:50]}")

        # --- 3단계: 파일 다운로드 요청 ---
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(DOWNLOAD_URL, data=download_payload, verify=True)
        download_response.raise_for_status()

        # Load Excel format
        df = pd.read_excel(io.BytesIO(download_response.content))
        
        # --- 4단계: 데이터 가공 (순매수 거래대금 상위 20) ---
        
        # 실제 순매수 컬럼명 탐색 및 정렬
        sort_col = None
        NET_VALUE_KEYWORDS = ['순매수', '거래대금']
        
        for col in df.columns:
            if all(keyword in str(col).lower() for keyword in NET_VALUE_KEYWORDS):
                sort_col = col
                break
        
        if sort_col is None:
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                sort_col = numeric_cols[-1] 
                print(f"   -> ⚠️ 순매수 컬럼을 찾을 수 없어 '{sort_col}' 기준으로 임시 정렬합니다.")
            else:
                 raise ValueError("DataFrame에 순매수대금 컬럼을 찾을 수 없습니다.")
        else:
            print(f"   -> 순매수 컬럼 '{sort_col}' 기준으로 정렬합니다.")


        df_sorted = df.sort_values(by=sort_col, ascending=False)
        df_top20 = df_sorted.head(20).copy() 
        
        df_top20 = df_top20[['종목명', sort_col]].rename(columns={sort_col: '순매수대금(천원)'})
        
        return df_top20
