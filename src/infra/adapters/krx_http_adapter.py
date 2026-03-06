# infra/adapters/krx_http_adapter.py
import requests
import datetime
import os
from typing import Optional

from core.ports.krx_data_port import KrxDataPort
from core.domain.models import Market, Investor

class KrxHttpAdapter(KrxDataPort):
    """requests.Session을 사용한 KRX 데이터 어댑터
    
    직접 HTTP 요청을 통해 KRX 상한가 소스 및 가격 정보를 직접 스크래핑합니다.
    """
    
    BASE_URL = "https://data.krx.co.kr"

    def __init__(self):
        """KrxHttpAdapter 초기화"""
        super().__init__()
        self.session = requests.Session()
        self.otp_url = 'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        self.download_url = 'https://data.krx.co.kr/comm/fileDn/download_excel/download.cmd'
        
        # User-Agent 설정
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        self.session.headers.update({'User-Agent': self.user_agent})
        
        # 로그인 정보 (환경변수 또는 하드코딩된 값 사용 가능)
        self.username = os.getenv("KRX_USERNAME", "zeya9643")
        self.password = os.getenv("KRX_PASSWORD", "chlwltjr43!")
        
        # 세션 초기화 여부
        self.is_logged_in = False

    def _login(self) -> None:
        """KRX 정보데이터시스템 로그인 후 세션 쿠키(JSESSIONID)를 갱신합니다.
        
        로그인 흐름:
          1. GET MDCCOMS001.cmd  → 초기 JSESSIONID 발급
          2. GET login.jsp       → iframe 세션 초기화
          3. POST MDCCOMS001D1.cmd → 실제 로그인
          4. CD011(중복 로그인) → skipDup=Y 추가 후 재전송
        """
        _LOGIN_PAGE = f"{self.BASE_URL}/contents/MDC/COMS/client/MDCCOMS001.cmd"
        _LOGIN_JSP  = f"{self.BASE_URL}/contents/MDC/COMS/client/view/login.jsp?site=mdc"
        _LOGIN_URL  = f"{self.BASE_URL}/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
        
        print("  [KrxHttp] 직접 요청으로 세션 초기화 (로그인)...")
        try:
            # 1 & 2. 초기 세션 발급
            self.session.get(_LOGIN_PAGE, timeout=15)
            self.session.get(_LOGIN_JSP, headers={"Referer": _LOGIN_PAGE}, timeout=15)
            
            payload = {
                "mbrNm": "", "telNo": "", "di": "", "certType": "",
                "mbrId": self.username, "pw": self.password,
            }
            headers = {"Referer": _LOGIN_PAGE}
            
            # 3. 로그인 POST
            resp = self.session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
            data = resp.json()
            error_code = data.get("_error_code", "")
            
            # 4. CD011 중복 로그인 처리
            if error_code == "CD011":
                print("  [KrxHttp] 중복 로그인 감지. 재로그인 시도...")
                payload["skipDup"] = "Y"
                resp = self.session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
                data = resp.json()
                error_code = data.get("_error_code", "")
                
            if error_code == "CD001":
                print(f"  [KrxHttp] 세션 획득 완료 (회원번호: {data.get('MBR_NO', '')})")
                self.is_logged_in = True
            else:
                print(f"  [KrxHttp] 로그인 에러: {data}")
                self.is_logged_in = False
                
            # 기본 쿠키 세팅
            self.session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
            self.session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
            
        except Exception as e:
            print(f"  [KrxHttp] 로그인 요청 실패: {e}")
            self.is_logged_in = False

    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """직접 세션을 사용하여 데이터(Excel Bytes)를 가져옵니다.
        """
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
        
        print(f"  [KrxHttp] {target_date} {market.value} {investor.value} 데이터 수집 시작")
        
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                # 1. 세션 로그인 확인
                if not self.is_logged_in:
                    self._login()
                
                # 2. 헤더 설정 (최신화)
                self.session.headers.update({
                    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
                    'Origin': 'https://data.krx.co.kr',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                })
                
                # 3. OTP 발급 요청
                otp_params = self._create_otp_params(market, investor, target_date)
                otp_response = self.session.post(self.otp_url, data=otp_params)
                otp_code = otp_response.text.strip()
                
                if len(otp_code) < 10 or 'LOGOUT' in otp_code:
                     if attempt < max_retries:
                         print(f"  [KrxHttp] 세션 만료/LOGOUT 감지. 세션 재설정 후 재시도합니다...")
                         self.is_logged_in = False
                         continue # 재시도
                     else:
                        raise ConnectionError(f"OTP 발급 실패: {otp_code[:50]}...")
                
                print(f"  [KrxHttp] OTP 발급 성공")

                # 4. 파일 다운로드 요청
                download_response = self.session.post(
                    self.download_url,
                    data={'code': otp_code}
                )
                
                file_bytes = download_response.content
                
                if len(file_bytes) == 0:
                    print(f"  [KrxHttp] 경고: 0 바이트 파일 다운로드됨")
                else:
                    print(f"  [KrxHttp] 다운로드 완료 ({len(file_bytes)} bytes)")
                
                return file_bytes
                
            except Exception as e:
                print(f"  [KrxHttp] 데이터 수집 중 오류: {e}")
                if attempt < max_retries:
                     print("  [KrxHttp] 예외 발생으로 인한 재시도...")
                     self.is_logged_in = False
                     continue
                raise
            

    def _create_otp_params(self, market: Market, investor: Investor, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 파라미터를 생성합니다.
        
        Args:
            market: 시장 구분
            investor: 투자자 구분
            target_date: 대상 날짜 (YYYYMMDD)
            
        Returns:
            dict: OTP 요청 파라미터
        """
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
            raise ValueError(f"Unsupported market: {market}")
        
        if investor == Investor.INSTITUTIONS:
            params['invstTpCd'] = '7050'
        elif investor == Investor.FOREIGNER:
            params['invstTpCd'] = '9000'
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
        
        return params
