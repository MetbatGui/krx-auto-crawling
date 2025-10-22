# infra/adapters/krx_http_adapter.py
import cloudscraper
import datetime
import os
import time
from dotenv import load_dotenv
import requests.exceptions

from ports.krx_data_port import KrxDataPort

load_dotenv()

OTP_URL = os.getenv('KRX_OTP_URL')
DOWNLOAD_URL = os.getenv('KRX_DOWNLOAD_URL')

# 기존 DailyNetValueCrawler가 'Adapter'가 됩니다.
# 상속받는 클래스가 KrxDataPort로 변경되었습니다.
class KrxHttpAdapter(KrxDataPort):
    """
    KrxDataPort의 '구현체(Adapter)'입니다.
    cloudscraper와 HTTP(OTP)를 사용하여 KRX에서
    실제 데이터를 가져옵니다.
    
    (기존 DailyNetValueCrawler의 Docstring과 동일)
    """
    
    def __init__(self):
        """(기존 __init__과 동일)"""
        super().__init__()
        self.scraper = cloudscraper.create_scraper()
        
    def create_otp_params(self, market: str, investor: str, target_date: str) -> dict:
        """(기존 create_otp_params와 동일)"""
        
        market = market.upper()
        investor = investor.lower()
        
        params = {
            'locale': 'ko_KR',
            'invstTpCd': '',
            'strtDd': target_date,
            'endDd': target_date,
            'share': '1',
            'money': '3',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
        }
        
        if market == 'KOSPI':
            params['mktId'] = 'STK'
        elif market == 'KOSDAQ':
            params['mktId'] = 'KSQ'
            params['segTpCd'] = 'ALL' 
        else:
            raise ValueError(f"Unsupported market ID: {market}")

        if investor == 'institutions':
            params['invstTpCd'] = '7050'
        elif investor == 'foreigner':
            params['invstTpCd'] = '9000'
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
            
        return params
    
    # 'crawl' 메서드 이름을 'fetch_net_value_data'로 변경 (Port 준수)
    def fetch_net_value_data(
        self, 
        market: str, 
        investor: str, 
        date_str: str = None
    ) -> bytes:
        """
        (기존 crawl 메서드의 Docstring과 동일)
        """
        
        # 1. 날짜 설정 및 파라미터 준비 (기존 로직 동일)
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
            
        market = market.upper()
        investor = investor.lower()
            
        time.sleep(1) 
        
        otp_payload = self.create_otp_params(market, investor, target_date)
        
        print(f"  [Adapter:KrxHttp] Fetching raw data for {market} ({investor}) on {target_date}")
        
        # --- 2단계: OTP 생성 요청 (기존 로직 동일) ---
        otp_response = self.scraper.post(OTP_URL, data=otp_payload, verify=True)
        otp_response.raise_for_status() 
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            raise ConnectionError(f"OTP acquisition failed. Response: {otp_code[:50]}")

        # --- 3단계: 파일 다운로드 요청 (기존 로직 동일) ---
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(DOWNLOAD_URL, data=download_payload, verify=True)
        download_response.raise_for_status()

        # --- 4단계: 원본 바이트 반환 (기존 로직 동일) ---
        return download_response.content