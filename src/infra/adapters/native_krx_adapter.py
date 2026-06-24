"""KRX 통합 데이터 어댑터 (데이터 조회 및 가격 조회 통합)"""
import os
import requests
import datetime
import time
import json
from typing import Optional, List, Tuple

from core.ports.krx_data_port import KrxDataPort
from core.ports.price_data_port import PriceDataPort, StockPriceInfo
from core.domain.models import Market, Investor

class NativeKrxAdapter(KrxDataPort, PriceDataPort):
    """KRX API를 직접 호출하여 순매수 데이터와 과거 가격 데이터를 통합 조회하는 어댑터"""
    
    BASE_URL = "https://data.krx.co.kr"
    
    def __init__(self):
        """NativeKrxAdapter 초기화"""
        super().__init__()
        self.session = requests.Session()
        self.otp_url = f'{self.BASE_URL}/comm/fileDn/GenerateOTP/generate.cmd'
        self.download_url = f'{self.BASE_URL}/comm/fileDn/download_excel/download.cmd'
        
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
            print("  [Adapter:NativeKrx] 경고: KRX_USERNAME, KRX_PASSWORD 환경변수가 설정되지 않았습니다.")
            
        self.is_logged_in = False
        
        # 캐시 설정 (가격 조회용)
        self.cache_dir = "output/cache"
        self.cache_file = os.path.join(self.cache_dir, "price_cache.json")
        self.cache_data = self._load_cache()
            
        print("[Adapter:NativeKrx] 통합 어댑터 초기화 완료")

    # =========================================================================
    # 세션 관리 및 공통 기능 영역
    # =========================================================================

    def _login(self) -> None:
        """KRX 정보데이터시스템 로그인 후 공용 세션 쿠키(JSESSIONID, mdc.client_session) 갱신"""
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
                print(f"  [NativeKrx] 세션 획득 완료 (회원번호: {data.get('MBR_NO', '')})")
                self.is_logged_in = True
            else:
                print(f"  [NativeKrx] 로그인 에러: {data}")
                self.is_logged_in = False
                
            # 기본 쿠키 세팅
            self.session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
            self.session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
            
        except Exception as e:
            print(f"  [NativeKrx] 로그인 요청 실패: {e}")
            self.is_logged_in = False

    def _parse_num(self, val: str) -> float:
        try:
            return float(val.replace(',', ''))
        except (ValueError, TypeError):
            return 0.0

    # =========================================================================
    # 투자자별 순매수 데이터 다운로드 영역 (KrxDataPort)
    # =========================================================================

    def _create_otp_params(self, market: Market, investor: Investor, target_date: str) -> dict:
        """KRX 순매수내역 엑셀 다운로드를 위한 OTP 요청 파라미터 생성 (MDCSTAT02401)"""
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

    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """KrxDataPort 구현: 직접 세션을 사용하여 데이터(Excel Bytes)를 가져옵니다."""
        target_date = date_str or datetime.date.today().strftime('%Y%m%d')
        print(f"  [NativeKrx] {target_date} {market.value} {investor.value} 다운로드 시작")
        
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                if not self.is_logged_in:
                    self._login()
                
                # OTP 발급
                otp_params = self._create_otp_params(market, investor, target_date)
                otp_response = self.session.post(self.otp_url, data=otp_params, timeout=15)
                otp_code = otp_response.text.strip()
                
                if len(otp_code) < 10 or 'LOGOUT' in otp_code:
                     if attempt < max_retries:
                         print(f"  [NativeKrx] OTP 세션 만료(LOGOUT). 재로그인 시도...")
                         self.is_logged_in = False
                         continue
                     else:
                        raise ConnectionError(f"OTP 발급 실패: {otp_code[:50]}...")
                
                # 파일 다운로드
                download_response = self.session.post(
                    self.download_url,
                    data={'code': otp_code},
                    timeout=30
                )
                
                file_bytes = download_response.content
                if len(file_bytes) == 0:
                    print(f"  [NativeKrx] 경고: 0 바이트 파일 다운로드됨")
                else:
                    print(f"  [NativeKrx] 다운로드 성공 ({len(file_bytes)} bytes)")
                
                return file_bytes
                
            except Exception as e:
                print(f"  [NativeKrx] 다운로드 에러: {e}")
                if attempt < max_retries:
                     print("  [NativeKrx] 재시도...")
                     self.is_logged_in = False
                     continue
                raise

    # =========================================================================
    # 과거 가격 (신고가 지표) 조회 영역 (PriceDataPort)
    # =========================================================================

    def _load_cache(self) -> dict:
        """가격 조회 로컬 캐시 로드"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"  [NativeKrx] 캐시 로드 실패: {e}")
        return {}
        
    def _save_cache(self):
        """가격 조회 현재 캐시 상태 디스크 저장"""
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  [NativeKrx] 캐시 저장 에러: {e}")

    def get_full_ticker_map(self, date_str: Optional[str] = None) -> dict[str, str]:
        """전종목 시세 데이터를 조회하여 종목명 -> 종목코드 매핑을 반환합니다."""
        target_date = date_str or datetime.date.today().strftime('%Y%m%d')
        print(f"  [NativeKrx] 전종목 티커 매핑 조회 시작 ({target_date})")
        
        url = f"{self.BASE_URL}/comm/bldAttendant/getJsonData.cmd"
        payload = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': target_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        
        try:
            res = self.session.post(url, data=payload, timeout=15)
            if 'LOGOUT' in res.text:
                self._login()
                res = self.session.post(url, data=payload, timeout=15)
            
            data = res.json()
            output = data.get('OutBlock_1', []) or data.get('output', [])
            
            ticker_map = {}
            for row in output:
                name = row.get('ISU_ABBRV')
                ticker = row.get('ISU_SRT_CD')
                if name and ticker:
                    ticker_map[name] = ticker
            
            print(f"  [NativeKrx] 매핑 완료: {len(ticker_map)}개 종목 확보")
            return ticker_map
        except Exception as e:
            print(f"  [NativeKrx] 매핑 조회 실패: {e}")
            return {}

    def _get_isu_cd(self, ticker: str, date_str: str) -> Optional[str]:
        """단축 종목코드를 풀 종목코드로 변환 (MDCSTAT01501)"""
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
                 if not self.is_logged_in:
                     self._login()
                     res = self.session.post(url, data=payload, timeout=15)
                 
            if res.status_code != 200:
                print(f"  [NativeKrx] 종목 풀코드 조회 실패 (HTTP {res.status_code})")
                return None
                
            data = res.json()
            output = data.get('OutBlock_1', []) or data.get('output', [])
            for row in output:
                if row.get('ISU_SRT_CD') == ticker:
                    return row.get('ISU_CD')
                    
            print(f"  [NativeKrx] 코드 {ticker}를 마켓 데이터에서 찾을 수 없습니다.")
            return f"A{ticker}"
            
        except Exception as e:
            print(f"  [NativeKrx] {ticker} 풀코드 조회 오류: {e}")
            return f"A{ticker}"

    def _fetch_bulk_price_change(self, market_id: str, start_date: str, end_date: str) -> list[dict]:
        """특정 기간 동안의 전종목 시세 변동 데이터를 조회합니다 (MDCSTAT01801).
        
        Args:
            market_id (str): 'STK' (KOSPI), 'KSQ' (KOSDAQ), 'ALL'
            start_date (str): 시작일 (YYYYMMDD)
            end_date (str): 종료일 (YYYYMMDD)
            
        Returns:
            list[dict]: 종목별 변동 데이터 리스트.
        """
        url = f"{self.BASE_URL}/comm/bldAttendant/getJsonData.cmd"
        payload = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01801',
            'locale': 'ko_KR',
            'mktId': market_id,
            'strtDd': start_date,
            'endDd': end_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        
        try:
            res = self.session.post(url, data=payload, timeout=20)
            if 'LOGOUT' in res.text:
                self._login()
                res = self.session.post(url, data=payload, timeout=20)
            
            data = res.json()
            return data.get('OutBlock_1', []) or data.get('output', [])
        except Exception as e:
            print(f"  [NativeKrx] 벌크 시세 조회 실패 ({market_id}, {start_date}~{end_date}): {e}")
            return []

    def get_bulk_price_info(self, tickers: list[str], date_str: str) -> dict[str, StockPriceInfo]:
        """PriceDataPort 구현: 벌크 조회를 통해 여러 종목의 가격 정보(신고가 포함)를 한 번에 반환합니다."""
        print(f"  [NativeKrx] {len(tickers)}개 종목 벌크 가격 조회 시작 ({date_str})")
        
        target_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        cutoff_52w = (target_dt - datetime.timedelta(days=365)).strftime("%Y%m%d")
        
        # 1. 52주 기간 데이터 조회 (KOSPI, KOSDAQ 각각)
        # 52주 데이터는 현재일 포함 1년치
        kospi_52w = self._fetch_bulk_price_change('STK', cutoff_52w, date_str)
        kosdaq_52w = self._fetch_bulk_price_change('KSQ', cutoff_52w, date_str)
        
        # 2. 결과 맵 구성 (52주 최고가 및 당일 종가)
        price_map = {}
        all_market_data = kospi_52w + kosdaq_52w
        
        for row in all_market_data:
            ticker = row.get('ISU_SRT_CD')
            if ticker not in tickers:
                continue
                
            close = self._parse_num(row.get('TDD_CLSPRC', '0'))
            # 52주 최고가는 해당 기간의 '고가' 중 최대값
            high_52w = self._parse_num(row.get('TDD_HGPRC', '0'))
            
            # 벌크 데이터는 시작일~종료일 전체의 최고가를 한 행에 요약해서 줄 수도 있고, 
            # MDCSTAT01801의 경우 'HG_PRC'(고가) 컬럼이 해당 기간 내의 최고가를 의미함.
            high_52w_period = self._parse_num(row.get('HG_PRC', str(high_52w)))
            
            price_map[ticker] = {
                'close': close,
                'high_52w': high_52w_period,
                'ath': high_52w_period # 일단 ATH는 52주와 동일하게 초기화
            }

        # 3. 역사적 신고가 처리 (캐시 활용 + 필요 시 장기 벌크 조회)
        # 모든 종목을 2000년부터 조회하면 무거울 수 있으므로, 52주 신고가 근처인 종목만 정밀 검증하거나
        # 혹은 전체를 2년 단위로 끊어서 벌크 패치 ( pykrx는 이 방식을 씀 )
        
        # 여기서는 단순화를 위해 캐시된 ATH가 있다면 활용하고, 
        # 없으면 2000년부터 현재까지의 벌크 데이터를 한 번 더 가져옴 (매우 빠름)
        # KRX MDCSTAT01801은 장기 조회도 한 번에 리턴함.
        
        long_term_start = "20000101"
        kospi_ath = self._fetch_bulk_price_change('STK', long_term_start, date_str)
        kosdaq_ath = self._fetch_bulk_price_change('KSQ', long_term_start, date_str)
        
        for row in (kospi_ath + kosdaq_ath):
            ticker = row.get('ISU_SRT_CD')
            if ticker in price_map:
                ath = self._parse_num(row.get('HG_PRC', '0'))
                price_map[ticker]['ath'] = ath
                
                # 캐시 업데이트
                self.cache_data[ticker] = {
                    'last_updated': date_str,
                    'all_time_high': ath
                }
        
        self._save_cache()
        
        # 4. StockPriceInfo 객체로 변환
        result = {}
        for ticker in tickers:
            data = price_map.get(ticker)
            if data and data['close'] > 0:
                result[ticker] = StockPriceInfo(
                    ticker=ticker,
                    close_price=data['close'],
                    high_52w=data['high_52w'],
                    all_time_high=data['ath']
                )
        
        print(f"  [NativeKrx] 벌크 조회 완료: {len(result)}/{len(tickers)}개 성공")
        return result

    def get_price_info(self, ticker: str, date_str: str) -> Optional[StockPriceInfo]:
        """PriceDataPort 구현: 종목의 전체 혹은 1년치 가격 정보를 조회하여 최고가 정보 반환"""
        print(f"  [NativeKrx] {ticker} 가격 조회 ({date_str})...")
        
        try:
            target_date_dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            return None
            
        if not self.is_logged_in:
            self._login()
            
        isu_cd = self._get_isu_cd(ticker, date_str)
        if not isu_cd:
            return None
            
        url = f"{self.BASE_URL}/comm/bldAttendant/getJsonData.cmd"
        
        # 캐싱 최적화
        cached_info = self.cache_data.get(ticker)
        cached_ath = 0.0
        use_cache = False
        
        cutoff_52w = target_date_dt - datetime.timedelta(days=365)
        start_dt = datetime.datetime(1990, 1, 1)
        
        if cached_info and 'last_updated' in cached_info and 'all_time_high' in cached_info:
            try:
                last_updated_dt = datetime.datetime.strptime(cached_info['last_updated'], "%Y%m%d")
                if target_date_dt >= last_updated_dt:
                    use_cache = True
                    cached_ath = float(cached_info['all_time_high'])
                    start_dt = min(cutoff_52w, last_updated_dt + datetime.timedelta(days=1))
                    print(f"  [NativeKrx] 캐시 적용! (기존 역.신: {cached_ath:,.0f}, API 범위:{start_dt.strftime('%Y%m%d')}~)")
            except ValueError:
                pass
        
        date_chunks: List[Tuple[str, str]] = []
        cur_start = start_dt
        while cur_start <= target_date_dt:
            # KRX 차트는 최대 2년(730일) 단위만 허용
            cur_end = min(cur_start + datetime.timedelta(days=730), target_date_dt)
            date_chunks.append((cur_start.strftime("%Y%m%d"), cur_end.strftime("%Y%m%d")))
            cur_start = cur_end + datetime.timedelta(days=1)
            
        close_price = None
        all_time_highs = []
        recent_52w_highs = []
        
        for chunk_start, chunk_end in date_chunks:
            payload = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
                'locale': 'ko_KR',
                'isuCd': isu_cd,
                'isuSrtCd': ticker,
                'strtDd': chunk_start,
                'endDd': chunk_end,
                'adjStkPrc_isNo': 'Y',
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
            }
            
            try:
                resp = self.session.post(url, data=payload, timeout=15)
                if 'LOGOUT' in resp.text:
                    self._login()
                    resp = self.session.post(url, data=payload, timeout=15)
                    
                time.sleep(0.3)
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
                    if trd_dd_str == date_str:
                        close_price = self._parse_num(row.get('TDD_CLSPRC', '0'))
                        continue
                        
                    if trd_dt < target_date_dt:
                        all_time_highs.append(high_val)
                        if trd_dt >= cutoff_52w:
                            recent_52w_highs.append(high_val)
                            
            except Exception as e:
                print(f"  [NativeKrx] 차트 청크({chunk_start}~{chunk_end}) 로드 오류: {e}")
                time.sleep(1.0)
                continue
                
        if close_price is None or close_price <= 0:
            print(f"  [NativeKrx] {ticker} {date_str} 기준 종가(거래기록)가 없습니다.")
            return None
            
        if not all_time_highs and not use_cache:
            high_52w = 0.0
            all_time_high = 0.0
        else:
            max_fetched_high = max(all_time_highs) if all_time_highs else 0.0
            all_time_high = max(cached_ath, max_fetched_high)
            high_52w = max(recent_52w_highs) if recent_52w_highs else all_time_high
            
        # 갱신 캐시 저장
        self.cache_data[ticker] = {
            'last_updated': target_date_dt.strftime("%Y%m%d"),
            'all_time_high': all_time_high
        }
        self._save_cache()
            
        print(f"  [NativeKrx] {ticker} 계산: 종가 {close_price:,.0f}, 52신 {high_52w:,.0f}, 역신 {all_time_high:,.0f}")

        return StockPriceInfo(
            ticker=ticker,
            close_price=close_price,
            high_52w=high_52w,
            all_time_high=all_time_high
        )
