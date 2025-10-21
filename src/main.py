import pandas as pd
import cloudscraper
import io
import datetime
import os
import time
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from typing import Dict, Any

# 1. 환경 변수 로드 (Load environment variables)
load_dotenv()

# KRX API URLs (환경 변수에서 로드, .env 파일에 설정 필요)
OTP_URL = os.getenv('KRX_OTP_URL')
DOWNLOAD_URL = os.getenv('KRX_DOWNLOAD_URL')
OUTPUT_DIR = 'krx_output' # 엑셀 파일을 저장할 디렉토리

# --- Base Crawler Definition (추상 기본 클래스) ---
class Crawler(ABC):
    """모든 크롤러가 상속받아야 하는 추상 기본 클래스입니다."""
    @abstractmethod
    def crawl(self, **kwargs) -> pd.DataFrame:
        """크롤링을 실행하고 가공된 데이터를 DataFrame으로 반환합니다."""
        pass
    
    def get_info(self) -> str:
        """크롤러의 정보를 반환합니다."""
        return f"Crawler: {self.__class__.__name__}"


# --- DailyNetValueCrawler Implementation (사용자가 제공한 코드) ---

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
        
        print(f"   -> Crawling {market} ({investor}) for {target_date}")
        
        # --- 2단계: OTP 생성 요청 ---
        if not OTP_URL:
            # 실제 KRX API를 사용하려면 KRX_OTP_URL 환경 변수를 설정해야 합니다.
            raise EnvironmentError("KRX_OTP_URL is not set in environment variables.")
            
        otp_response = self.scraper.post(OTP_URL, data=otp_payload, verify=True)
        otp_response.raise_for_status() 
        otp_code = otp_response.text

        if not otp_code or len(otp_code) < 50:
            raise ConnectionError(f"OTP acquisition failed. Check date or market status. Response snippet: {otp_code[:50]}")

        # --- 3단계: 파일 다운로드 요청 ---
        if not DOWNLOAD_URL:
            # 실제 KRX API를 사용하려면 KRX_DOWNLOAD_URL 환경 변수를 설정해야 합니다.
            raise EnvironmentError("KRX_DOWNLOAD_URL is not set in environment variables.")
            
        download_payload = {'code': otp_code}
        download_response = self.scraper.post(DOWNLOAD_URL, data=download_payload, verify=True)
        download_response.raise_for_status()

        # Load Excel format
        df = pd.read_excel(io.BytesIO(download_response.content))
        
        # --- 4단계: 데이터 가공 (순매수 거래대금 상위 20) ---
        
        # 요청에 따라 순매수 컬럼명을 '거래대금_순매수'로 명확히 지정
        SORT_COL_NAME = '거래대금_순매수' 
        sort_col = None
        
        # 실제 KRX 파일에서 일치하는 컬럼명 찾기 (대소문자 무시 및 공백 제거)
        for col in df.columns:
            if str(col).strip().replace('_', '').lower() == SORT_COL_NAME.replace('_', '').lower():
                sort_col = col
                break
        
        if sort_col is None:
            # KRX 파일 컬럼명이 일치하지 않을 경우, 기존 로직으로 대체 컬럼을 찾습니다.
            NET_VALUE_KEYWORDS = ['순매수', '거래대금']
            for col in df.columns:
                 if all(keyword in str(col).lower() for keyword in NET_VALUE_KEYWORDS):
                    sort_col = col
                    print(f"   -> ⚠️ 요청하신 '{SORT_COL_NAME}' 컬럼을 찾을 수 없어, '{sort_col}' 기준으로 대체 정렬합니다.")
                    break

            if sort_col is None:
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    sort_col = numeric_cols[-1] 
                    print(f"   -> ⚠️ 순매수 컬럼을 찾을 수 없어, 마지막 숫자 컬럼 '{sort_col}' 기준으로 임시 정렬합니다.")
                else:
                    raise ValueError("DataFrame에 순매수대금 컬럼을 찾을 수 없습니다.")
        else:
            print(f"   -> 순매수 컬럼 '{sort_col}' 기준으로 정렬합니다.")


        df_sorted = df.sort_values(by=sort_col, ascending=False)
        df_top20 = df_sorted.head(20).copy() 
        
        # 요청에 따라 컬럼 이름을 '거래대금_순매수'로 통일 (천원 제거)
        df_top20 = df_top20[['종목명', sort_col]].rename(columns={sort_col: SORT_COL_NAME})
        
        return df_top20


# --- Data Storage Utility ---

def save_to_excel(all_results: Dict[str, pd.DataFrame], target_date: str):
    """
    수집된 4가지 데이터를 각각 별도의 엑셀 파일로 저장합니다.
    파일 이름 형식: <날짜><시장><투자자>순매수.xlsx (예: 20251020코스피외국인순매수.xlsx)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\n[💾 Saving results to 4 separate Excel files in {OUTPUT_DIR}/]")

    # 파일 이름 매핑을 정의합니다. (key: 'KOSPI_foreigner' -> value: '코스피외국인')
    NAME_MAP = {
        'KOSPI_foreigner': '코스피외국인',
        'KOSPI_institutions': '코스피기관',
        'KOSDAQ_foreigner': '코스닥외국인',
        'KOSDAQ_institutions': '코스닥기관',
    }
    
    saved_count = 0
    
    for key, df in all_results.items():
        if not df.empty:
            try:
                # 1. 파일 이름 생성: <날짜><시장><투자자>순매수.xlsx
                korean_name_part = NAME_MAP.get(key, key.replace('_', '').title())
                filename = f"{target_date}{korean_name_part}순매수.xlsx"
                filepath = os.path.join(OUTPUT_DIR, filename)

                df_to_save = df.copy()
                if '거래대금_순매수' in df_to_save.columns:
                     # 쉼표 포맷팅을 위해 문자열로 변환합니다.
                    df_to_save['거래대금_순매수'] = df_to_save['거래대금_순매수'].apply(lambda x: f"{x:,}")

                # 2. DataFrame을 별도의 엑셀 파일로 저장 (Sheet 이름은 기본값 'Sheet1')
                df_to_save.to_excel(filepath, index=False)
                print(f"   -> File '{filename}' successfully saved.")
                saved_count += 1

            except Exception as e:
                print(f"❌ Error saving file for {key}: {e}")
        else:
            print(f"   -> Task '{key}' skipped (Empty DataFrame).")

    if saved_count > 0:
        print(f"✅ Total {saved_count} files successfully saved.")
    else:
        print(f"⚠️ No files were saved.")


# --- Main Execution Logic ---

def main():
    """4가지 시장/투자자 조합에 대해 크롤링을 실행하고 결과를 출력합니다."""
    
    # 예시 날짜 설정: KRX는 영업일 기준으로만 데이터를 제공합니다.
    # 테스트를 위해서는 실제 영업일의 과거 날짜(YYYYMMDD)를 사용하세요.
    TARGET_DATE = datetime.date.today().strftime('%Y%m%d') # 기본값: 오늘 날짜
    
    CRAWL_COMBINATIONS = [
        ("KOSPI", "foreigner"),
        ("KOSPI", "institutions"),
        ("KOSDAQ", "foreigner"),
        ("KOSDAQ", "institutions"),
    ]
    
    print(f"--- Starting KRX 4-Way Daily Net Value Crawl (Target Date: {TARGET_DATE}) ---")

    try:
        # 크롤러 인스턴스 단일 생성
        crawler = DailyNetValueCrawler()
        
        all_results = {}
        
        for market, investor in CRAWL_COMBINATIONS:
            key = f"{market}_{investor}"
            print(f"\n[Task: {key}]")
            
            try:
                # 크롤러의 crawl 메서드를 재사용하며 각 조합에 대한 인자를 전달하여 데이터 수집
                df = crawler.crawl(market=market, investor=investor, date_str=TARGET_DATE)
                all_results[key] = df
                
                print(f"✅ Success: {key} - Collected {len(df)} records.")
                print(f"--- Top 5 Data for {key} ---")
                # Pandas DataFrame의 상위 5개 항목만 출력
                print(df.head().to_markdown(index=False, numalign="left", stralign="left")) 
                
            except Exception as e:
                print(f"❌ Failed: {key} - Error: {e}")
                all_results[key] = pd.DataFrame() # 실패 시 빈 DataFrame 저장
                
        # 크롤링 성공/실패 여부와 관계없이 결과를 엑셀로 저장 시도
        if any(not df.empty for df in all_results.values()):
            save_to_excel(all_results, TARGET_DATE)
        else:
            print("\n⚠️ No data collected successfully. Skipping Excel save.")
                
    except Exception as e:
        print(f"\n🚨 Critical initialization error: {e}")
        return

    print("\n--- All Crawl Tasks Complete ---")
    
if __name__ == '__main__':
    main()
