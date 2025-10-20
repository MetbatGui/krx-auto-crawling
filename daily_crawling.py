import pandas as pd
import cloudscraper
import io
import datetime
from abc import ABC, abstractmethod

# KRX API URL 정의
OTP_URL = 'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
DOWNLOAD_URL = 'https://data.krx.co.kr/comm/fileDn/download_excel/download.cmd'

class Crawler(ABC):
    @abstractmethod
    def crawl(self) -> pd.DataFrame:
        pass

class DailyNetValueCrawler(Crawler):
    
    def __init__(self, market: str, investor: str, date_str: str = None):
        super().__init__()
        self.market = market.upper()
        self.investor = investor.lower()
        
        # 날짜가 지정되지 않으면 오늘 날짜 사용
        if date_str is None:
            # 현재 시간을 기준으로 오늘 날짜를 YYYYMMDD 포맷으로 설정
            self.target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            self.target_date = date_str
            
        # Cloudscraper 인스턴스 생성 및 클래스 멤버 변수로 저장
        self.scraper = cloudscraper.create_scraper()
        
    def create_otp_params(self) -> dict:
        params = {
            'locale': 'ko_KR',
            'invstTpCd': '',
            'strtDd': self.target_date,
            'endDd': self.target_date,
            'share': '1',
            'money': '3',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
        }
        
        # 1. 시장 구분 (market_id)
        if self.market == 'KOSPI':
            params['mktId'] = 'STK'
        elif self.market == 'KOSDAQ':
            params['mktId'] = 'KSQ'
            params['segTpCd'] = 'ALL' # 코스닥은 segTpCd 필요
        else:
            raise ValueError(f"지원하지 않는 시장 ID: {self.market}. KOSPI 또는 KOSDAQ을 사용하세요.")

        # 2. 투자자 구분 (invstTpCd)
        if self.investor == 'institutions':
            params['invstTpCd'] = '7050' # 기관
        elif self.investor == 'foreigner':
            params['invstTpCd'] = '9000' # 외국인
        else:
            raise ValueError(f"지원하지 않는 투자자 유형: {self.investor}. institutions 또는 foreigner를 사용하세요.")
            
        return params
    
    def crawl(self) -> pd.DataFrame:
        otp_payload = self.create_otp_params()
        
        # --- 1단계: OTP 생성 요청 ---
        otp_response = self.scraper.post(OTP_URL, data=otp_payload, verify=True)
        otp_response.raise_for_status()
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            raise ConnectionError(f"OTP acquisition failed. KRX response: {otp_code[:100]}")

        # --- 2단계: 파일 다운로드 요청 및 Pandas 로드 ---
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(DOWNLOAD_URL, data=download_payload, verify=True)
        download_response.raise_for_status()

        # Load Excel format (KRX standard)
        df = pd.read_excel(io.BytesIO(download_response.content)
                           # KRX 엑셀 파일은 종종 첫 행을 건너뛰어야 할 수 있으나, 여기서는 생략
                           ) 
        
        # --- 3단계: 데이터 가공 (순매수 거래대금 상위 20) ---
        
        # 1. Sort: Descending by '거래대금_순매수'
        df_sorted = df.sort_values(by='거래대금_순매수', ascending=False)
        
        # 2. Cut: Extract top 20 data points
        df_top20 = df_sorted.head(20)
        
        # 3. Select: Keep only '종목명' and '거래대금_순매수'
        # 이 단계에서 데이터프레임을 최종적으로 정리합니다.
        df_top20 = df_top20[['종목명', '거래대금_순매수']]
        
        return df_top20

# --- 4가지 경우의 데이터 수집 및 저장 실행 ---
if __name__ == '__main__':
    # 현재 날짜 (KST 기준)
    today = datetime.date.today().strftime('%Y%m%d')

    # 테스트를 위한 고정 날짜 (20251017) 또는 오늘 날짜 (today) 사용
    target_date_for_crawling = "20251020" 

    MARKETS = ["KOSPI", "KOSDAQ"]
    INVESTORS = ["foreigner", "institutions"]
    
    # 4개의 DataFrame을 저장할 딕셔너리
    all_dfs = {}

    print(f"--- KRX Net Value Top 20 Crawler Start ({target_date_for_crawling}) ---")
    
    # 데이터 수집 루프
    for market in MARKETS:
        for investor in INVESTORS:
            key = f"{market}_{investor}"
            
            try:
                print(f"\n[🚀 Collecting: {market} - {investor}]")
                
                scraper_instance = DailyNetValueCrawler(
                    market=market,
                    investor=investor,
                    date_str=target_date_for_crawling
                )
                
                df_result = scraper_instance.crawl()
                all_dfs[key] = df_result
                
                print(f"✅ Success: {key} data collected ({len(df_result)} records).")
                
            except Exception as e:
                print(f"❌ Failed: {key} crawling failed: {e}")
                all_dfs[key] = pd.DataFrame() 
    
    # --- 파일 저장 루프 ---
    print("\n\n================================================")
    print("💾 Saving collected data to separate Excel files...")
    print("================================================")
    
    # 파일명 매핑을 위한 딕셔너리
    market_map = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
    investor_map = {"foreigner": "외국인", "institutions": "기관"}

    for key, df in all_dfs.items():
        if df.empty:
            print(f"⚠️ Skipping file saving for {key} (DataFrame is empty).")
            continue
            
        # 키 분리: 예시) KOSPI_foreigner -> ['KOSPI', 'foreigner']
        market_en, investor_en = key.split('_')
        
        # 한글 이름 변환
        market_kr = market_map.get(market_en)
        investor_kr = investor_map.get(investor_en)
        
        # 파일명 생성: 20251017코스피외국인순매수.xlsx
        filename = f"{target_date_for_crawling}{market_kr}{investor_kr}순매수.xlsx"
        
        try:
            # Excel 파일로 저장 (인덱스 제외)
            df.to_excel(filename, index=False)
            print(f"✅ Saved: '{filename}'")
            
        except Exception as e:
            print(f"❌ Error saving '{filename}': {e}")

    print("\n--- All operations complete. ---")
