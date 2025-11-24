# infra/adapters/krx_http_adapter.py
import cloudscraper
import datetime
import os
import time
# infra/adapters/krx_http_adapter.py
import cloudscraper
import datetime
import os
import time
from dotenv import load_dotenv
from typing import Optional

from core.ports.krx_data_port import KrxDataPort
from core.domain.models import Market, Investor

class KrxHttpAdapter(KrxDataPort):
    """KrxDataPort의 구현체 (Adapter).

    cloudscraper와 HTTP(OTP)를 사용하여 KRX에서 실제 데이터를 가져옵니다.

    Attributes:
        scraper (cloudscraper.CloudScraper): CloudScraper 인스턴스
        otp_url (str): OTP 발급 URL
        download_url (str): 데이터 다운로드 URL
    """
    
    def __init__(self):
        """KrxHttpAdapter 초기화.

        Raises:
            EnvironmentError: 필수 환경 변수가 설정되지 않은 경우
        """
        super().__init__()
        self.scraper = cloudscraper.create_scraper()
        # KRX 403 Forbidden 방지를 위한 헤더 설정
        self.scraper.headers.update({
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # 환경 변수 로드 (인스턴스 생성 시점)
        self.otp_url = os.getenv('KRX_OTP_URL')
        self.download_url = os.getenv('KRX_DOWNLOAD_URL')

        if not self.otp_url or not self.download_url:
            raise EnvironmentError("KRX_OTP_URL or KRX_DOWNLOAD_URL is not set in environment variables.")
        
    def _create_otp_params(self, market: Market, investor: Investor, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 페이로드를 생성합니다."""
        
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
        
        if market == Market.KOSPI:
            params['mktId'] = 'STK'
        elif market == Market.KOSDAQ:
            params['mktId'] = 'KSQ'
            params['segTpCd'] = 'ALL' 
        else:
            raise ValueError(f"Unsupported market ID: {market}")

        if investor == Investor.INSTITUTIONS:
            params['invstTpCd'] = '7050'
        elif investor == Investor.FOREIGNER:
            params['invstTpCd'] = '9000'
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
            
        return params
    
    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """지정된 조건의 투자자별 순매수 원본 엑셀(bytes)을 가져옵니다.

        Args:
            market: 시장 구분 (KOSPI, KOSDAQ)
            investor: 투자자 구분 (외국인, 기관)
            date_str: 대상 날짜 (YYYYMMDD)

        Returns:
            다운로드된 엑셀 파일의 바이너리 데이터

        Raises:
            ConnectionError: OTP 발급 또는 다운로드 실패 시
        """
        
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
            
        time.sleep(1) 
        
        otp_payload = self._create_otp_params(market, investor, target_date)
        
        print(f"  [Adapter:KrxHttp] Fetching raw data for {market.value} ({investor.value}) on {target_date}")
        
        # OTP 생성 요청
        otp_response = self.scraper.post(self.otp_url, data=otp_payload, verify=True)
        otp_response.raise_for_status() 
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            raise ConnectionError(f"OTP acquisition failed. Response: {otp_code[:50]}")

        # 파일 다운로드 요청
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(self.download_url, data=download_payload, verify=True)
        download_response.raise_for_status()

        return download_response.content