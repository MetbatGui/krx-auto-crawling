"""KRX 직접 통신 기반 가격 조회 어댑터"""
import os
import requests
import datetime
import time
from typing import Optional, List, Tuple
from core.ports.price_data_port import PriceDataPort, StockPriceInfo

class NativeKrxPriceAdapter(PriceDataPort):
    """KRX API(MDCSTAT)를 직접 호출하여 가격 데이터를 조회하는 어댑터
    
    1990년부터 현재까지의 차트 데이터를 조회하여 (MDCSTAT01701)
    종목의 종가, 52주 신고가, 역사적 신고가를 계산합니다.
    """
    
    BASE_URL = "https://data.krx.co.kr"
    
    def __init__(self):
        """NativeKrxPriceAdapter 초기화"""
        self.session = requests.Session()
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': self.BASE_URL,
            'Referer': f'{self.BASE_URL}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
            'X-Requested-With': 'XMLHttpRequest'
        })
        self.username = os.getenv("KRX_USERNAME")
        self.password = os.getenv("KRX_PASSWORD")
        if not self.username or not self.password:
            print("  [NativeKrxPrice] 경고: KRX_USERNAME, KRX_PASSWORD 환경변수가 없습니다. (데이터 조회 불가)")
            
        print("[Adapter:NativeKrxPrice] 초기화 완료")
        
    def _login(self) -> None:
        """KRX 정보데이터시스템 로그인 후 세션 쿠키(JSESSIONID)를 갱신합니다."""
        _LOGIN_PAGE = f"{self.BASE_URL}/contents/MDC/COMS/client/MDCCOMS001.cmd"
        _LOGIN_JSP  = f"{self.BASE_URL}/contents/MDC/COMS/client/view/login.jsp?site=mdc"
        _LOGIN_URL  = f"{self.BASE_URL}/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
        
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
                payload["skipDup"] = "Y"
                resp = self.session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
                data = resp.json()
                error_code = data.get("_error_code", "")
                
            if error_code == "CD001":
                print(f"  [NativeKrxPrice] 세션 획득 완료 (회원번호: {data.get('MBR_NO', '')})")
            else:
                print(f"  [NativeKrxPrice] 로그인 에러: {data}")
                
            # 기본 쿠키 세팅
            self.session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
            self.session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
            
        except Exception as e:
            print(f"  [NativeKrxPrice] 로그인 요청 실패: {e}")
        
    def _parse_num(self, val: str) -> float:
        try:
            return float(val.replace(',', ''))
        except (ValueError, TypeError):
            return 0.0
            
    def _get_isu_cd(self, ticker: str, date_str: str) -> Optional[str]:
        """단축 종목코드(예: 005930)를 풀 종목코드(예: KR7005930003)로 변환합니다."""
        url = f"{self.BASE_URL}/comm/bldAttendant/getJsonData.cmd"
        payload = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': date_str,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        
        try:
            res = self.session.post(url, data=payload, timeout=15)
            if res.status_code != 200 or 'LOGOUT' in res.text:
                 # 세션 만료 시 재로그인 후 1회 재시도
                 print(f"  [NativeKrxPrice] 세션 만료 감지, 재로그인 시도...")
                 self._login()
                 res = self.session.post(url, data=payload, timeout=15)
                 
            if res.status_code != 200:
                print(f"  [NativeKrxPrice] KRX 마켓 데이터 조회 실패 (HTTP {res.status_code})")
                return None
                
            data = res.json()
            output = data.get('OutBlock_1', []) or data.get('output', [])
            
            for row in output:
                if row.get('ISU_SRT_CD') == ticker:
                    return row.get('ISU_CD')
                    
            print(f"  [NativeKrxPrice] 종목코드 {ticker}를 마켓 데이터에서 찾을 수 없습니다.")
            # fallback: 풀 코드를 못 찾으면 단축코드 + A 접두사 형태로 시도 (일부 API 허용)
            return f"A{ticker}"
            
        except Exception as e:
            print(f"  [NativeKrxPrice] 종목코드 변환 중 오류 발생: {e}")
            return f"A{ticker}"

    def get_price_info(self, ticker: str, date_str: str) -> Optional[StockPriceInfo]:
        """종목의 전체 기간 가격 정보를 조회하여 최고가 정보(StockPriceInfo)를 반환합니다.
        
        주의: KRX MDCSTAT01701 API는 1회에 최대 2년치(일별 데이터)만 허용하므로,
        1990년부터 대상 날짜까지 2년 단위로 청크를 나누어 API를 여러 번 호출합니다.
        """
        print(f"  [Adapter:NativeKrxPrice] {ticker} 가격 정보 조회 시작 ({date_str})...")
        
        try:
            target_date_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            print(f"  [Adapter:NativeKrxPrice] 잘못된 날짜 형식: {date_str}")
            return None
            
        isu_cd = self._get_isu_cd(ticker, date_str)
        if not isu_cd:
            return None
            
        # 차트 API (MDCSTAT01701)
        url = f"{self.BASE_URL}/comm/bldAttendant/getJsonData.cmd"
        
        # 2년 단위로 쪼개기
        start_dt = datetime.datetime(1990, 1, 1)
        date_chunks: List[Tuple[str, str]] = []
        cur_start = start_dt
        while cur_start <= target_date_dt:
            # 730일(2년) 단위
            cur_end = min(cur_start + datetime.timedelta(days=730), target_date_dt)
            date_chunks.append((cur_start.strftime("%Y%m%d"), cur_end.strftime("%Y%m%d")))
            cur_start = cur_end + datetime.timedelta(days=1)
            
        # 가장 최신 데이터 (target_date 당일 종가)
        close_price = None
        
        all_time_highs = []
        recent_52w_highs = []
        # 기준일(target_date) 당일은 52주/역사적 신고가 계산에서 제외 (당일 종가가 돌파했는지 보기 위함)
        cutoff_52w = target_date_dt - datetime.timedelta(days=365)
        
        for chunk_start, chunk_end in date_chunks:
            payload = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
                'locale': 'ko_KR',
                'isuCd': isu_cd,
                'isuSrtCd': ticker,
                'strtDd': chunk_start,
                'endDd': chunk_end,
                'adjStkPrc_isNo': 'Y', # 수정주가 적용 여부
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
            }
            
            try:
                resp = self.session.post(url, data=payload, timeout=15)
                if 'LOGOUT' in resp.text:
                    print(f"  [NativeKrxPrice] 세션 만료 감지, 재로그인 시도...")
                    self._login()
                    resp = self.session.post(url, data=payload, timeout=15)
                    
                time.sleep(0.3)  # KRX 서버 부하 방지용 딜레이
                
                output = resp.json().get('output', [])
                if not output:
                    continue
                    
                for row in output:
                    trd_dd_str = row.get('TRD_DD', '').replace('/', '')
                    if not trd_dd_str: continue
                    
                    try:
                        trd_dt = datetime.datetime.strptime(trd_dd_str, "%Y%m%d")
                    except ValueError:
                        continue
                        
                    high_val = self._parse_num(row.get('TDD_HGPRC', '0'))
                    
                    # 당일 데이터라면 종가만 별도 저장 (당일 고가도 포함해야 당일 상한가 돌파를 잡을 지는 업무 로직상 당일은 제외하는 기존 로직 유지)
                    if trd_dd_str == date_str:
                        close_price = self._parse_num(row.get('TDD_CLSPRC', '0'))
                        continue # 오늘 데이터의 High는 과거 히스토리 Max 계산에서 제외
                        
                    # 과거라면
                    if trd_dt < target_date_dt:
                        all_time_highs.append(high_val)
                        if trd_dt >= cutoff_52w:
                            recent_52w_highs.append(high_val)
                            
            except Exception as e:
                print(f"  [Adapter:NativeKrxPrice] 차트 청크({chunk_start}~{chunk_end}) 로드 중 오류: {e}")
                time.sleep(1.0)
                continue
                
        # 종가가 없으면 당일 데이터가 없는 것 (휴장일이거나 상장 폐지 등)
        if close_price is None or close_price <= 0:
            print(f"  [Adapter:NativeKrxPrice] {ticker} {date_str} 기준 종가 데이터가 없습니다.")
            return None
            
        if not all_time_highs:
            # 과거 데이터가 없는 경우 (오늘 막 상장한 1일차 종목)
            high_52w = 0.0
            all_time_high = 0.0
        else:
            all_time_high = max(all_time_highs)
            high_52w = max(recent_52w_highs) if recent_52w_highs else all_time_high
            
        print(f"  [Adapter:NativeKrxPrice] OK: {ticker} 조회 완료 (종가: {close_price:,.0f}, 전일기준 52주고가: {high_52w:,.0f}, 전일기준 역사적고가: {all_time_high:,.0f})")

        return StockPriceInfo(
            ticker=ticker,
            close_price=close_price,
            high_52w=high_52w,
            all_time_high=all_time_high
        )
