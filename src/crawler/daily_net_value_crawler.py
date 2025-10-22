import pandas as pd
import cloudscraper
import io
import datetime
import os
import time
from dotenv import load_dotenv
import requests.exceptions # 예외 처리를 명시하기 위해 import

from crawler.crawler import Crawler

load_dotenv()

OTP_URL = os.getenv('KRX_OTP_URL')
DOWNLOAD_URL = os.getenv('KRX_DOWNLOAD_URL')

class DailyNetValueCrawler(Crawler):
    """KRX에서 투자자별 일별 매매 현황 원본 엑셀 파일을 크롤링합니다.

    이 클래스는 KRX 데이터 시스템에 OTP를 요청하고, 결과로 나오는 엑셀 파일을
    순수한 바이트(bytes) 형태로 다운로드하는 책임을 갖습니다.
    Cloudflare 보호를 우회하기 위해 `cloudscraper`를 사용합니다.

    데이터의 파싱, 가공 (예: 상위 20개 종목 추출)은 이 클래스의 책임이 아니며,
    반환된 바이트를 사용하는 별도의 처리기(processor)에서 수행해야 합니다.

    Attributes:
        scraper (cloudscraper.CloudScraper): Cloudflare를 우회하기 위한
            `cloudscraper` 세션 인스턴스입니다.
    """
    
    def __init__(self):
        """DailyNetValueCrawler 인스턴스를 초기화합니다.

        초기화 시점에는 Cloudflare 우회를 위한 `cloudscraper` 인스턴스만
        공통 자원으로 생성합니다.
        """
        super().__init__()
        # Cloudscraper 인스턴스 생성 (Cloudflare 우회용)
        self.scraper = cloudscraper.create_scraper()
        
    def create_otp_params(self, market: str, investor: str, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 페이로드(dict)를 생성합니다.

        Args:
            market (str): 시장 구분 ('KOSPI' 또는 'KOSDAQ').
            investor (str): 투자자 구분 ('institutions' 또는 'foreigner').
            target_date (str): 조회 대상 날짜 (YYYYMMDD 형식).

        Returns:
            dict: KRX OTP 요청에 필요한 파라미터 딕셔너리.

        Raises:
            ValueError: 지원하지 않는 market 또는 investor 값이 입력된 경우.
        """
        
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
    
    def crawl(self, market: str, investor: str, date_str: str = None) -> bytes:
        """지정된 조건으로 KRX에서 원본 엑셀 파일(bytes)을 크롤링합니다.

        이 메소드는 다음 두 단계로 작동합니다:
        1. `create_otp_params`를 호출하여 OTP 요청 페이로드를 생성합니다.
        2. OTP를 요청하고, 반환된 코드로 실제 엑셀 파일을 다운로드합니다.

        Args:
            market (str): 시장 구분 ('KOSPI', 'KOSDAQ').
            investor (str): 투자자 구분 ('institutions', 'foreigner').
            date_str (str, optional): YYYYMMDD 형식의 날짜.
                None일 경우 오늘 날짜(UTC 기준 아님)를 사용합니다.

        Returns:
            bytes: 다운로드한 원본 엑셀 파일의 내용 (raw bytes).

        Raises:
            ConnectionError: OTP 발급에 실패했거나 유효하지 않은 코드를 받은 경우.
            requests.exceptions.HTTPError: OTP 요청 또는 다운로드 요청이
                HTTP 오류(4xx, 5xx)를 반환한 경우 (cloudscraper 내부의 requests).
        """
        
        # 1. 날짜 설정 및 파라미터 준비
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
            
        market = market.upper()
        investor = investor.lower()
            
        time.sleep(1) 
        
        otp_payload = self.create_otp_params(market, investor, target_date)
        
        print(f"  -> Fetching raw data for {market} ({investor}) on {target_date}")
        
        # --- 2단계: OTP 생성 요청 ---
        otp_response = self.scraper.post(OTP_URL, data=otp_payload, verify=True)
        otp_response.raise_for_status() # HTTP 오류 시 예외 발생
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            # KRX는 날짜가 잘못되거나 데이터가 없으면 HTML 페이지를 반환할 수 있음
            raise ConnectionError(f"OTP acquisition failed. Check date or market status. Response snippet: {otp_code[:50]}")

        # --- 3단계: 파일 다운로드 요청 ---
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(DOWNLOAD_URL, data=download_payload, verify=True)
        download_response.raise_for_status() # HTTP 오류 시 예외 발생

        # --- 4단계: 원본 바이트 반환 (수정된 부분) ---
        return download_response.content