from typing import List, Optional
import datetime
import pandas as pd
import io
import warnings

from core.domain.models import KrxData, Market, Investor
from core.ports.krx_data_port import KrxDataPort

class KrxFetchService:
    """
    KRX 데이터 수집 및 표준화를 담당하는 헬퍼 서비스.
    DailyRoutineService에서 호출하여 사용합니다.
    """

    def __init__(self, krx_port: KrxDataPort):
        self.krx_port = krx_port

    def fetch_all_data(self, date_str: Optional[str] = None) -> List[KrxData]:
        """
        정의된 모든 시장(KOSPI, KOSDAQ)과 투자자(외국인, 기관) 조합에 대해
        데이터를 수집하고 가공하여 KrxData 리스트로 반환합니다.
        """
        if date_str is None:
            date_str = datetime.date.today().strftime('%Y%m%d')

        results: List[KrxData] = []
        targets = [
            (Market.KOSPI, Investor.FOREIGNER),
            (Market.KOSPI, Investor.INSTITUTIONS),
            (Market.KOSDAQ, Investor.FOREIGNER),
            (Market.KOSDAQ, Investor.INSTITUTIONS),
        ]

        print(f"[Service:KrxFetch] {date_str} 데이터 수집 시작...")

        for market, investor in targets:
            try:
                # 1. 원본 데이터 수집 (Port 호출)
                raw_bytes = self.krx_port.fetch_net_value_data(market, investor, date_str)
                
                # 2. 데이터 가공 (내부 메서드 호출)
                df = self._process_krx_excel(raw_bytes)
                
                if df.empty:
                    print(f"  -> ⚠️ {market.value} {investor.value} 데이터가 비어있습니다 (휴장일 등).")
                    continue

                # 3. KrxData 객체 생성
                krx_data = KrxData(
                    market=market,
                    investor=investor,
                    date_str=date_str,
                    data=df
                )
                results.append(krx_data)
                print(f"  -> ✅ {market.value} {investor.value} 수집 및 가공 완료 ({len(df)}행)")

            except Exception as e:
                print(f"  -> 🚨 {market.value} {investor.value} 처리 중 오류 발생: {e}")

        return results

    def _process_krx_excel(self, excel_bytes: bytes) -> pd.DataFrame:
        """
        KRX 원본 CSV/Excel(bytes)을 파싱하여 순매수 상위 20개 DataFrame을 생성합니다.
        """
        if not excel_bytes:
            return pd.DataFrame()

        try:
            # CSV 파싱 (KRX는 EUC-KR 인코딩 사용)
            df = pd.read_csv(io.BytesIO(excel_bytes), encoding='euc-kr')
                
        except Exception as e:
            print(f"  [Service:KrxFetch] 🚨 CSV 파싱 중 오류: {e}")
            return pd.DataFrame()

        # --- 데이터 가공 (순매수 거래대금 상위 20) ---
        sort_col = None
        NET_VALUE_KEYWORDS = ['순매수', '거래대금']
        
        for col in df.columns:
            if all(keyword in str(col).lower() for keyword in NET_VALUE_KEYWORDS):
                sort_col = col
                break
        
        # 순매수대금 컬럼을 찾지 못한 경우, 마지막 숫자 컬럼을 사용
        if sort_col is None:
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                sort_col = numeric_cols[-1] 
                print(f"  [Service:KrxFetch] ⚠️ 순매수 컬럼을 찾을 수 없어 '{sort_col}' 기준으로 정렬.")
            else:
                print("  [Service:KrxFetch] 🚨 유효한 숫자 컬럼이 없어 가공 실패.")
                return pd.DataFrame()

        # 필수 컬럼 존재 여부 확인
        required_cols = ['종목코드', '종목명', sort_col]
        if not all(col in df.columns for col in required_cols):
            print(f"  [Service:KrxFetch] 🚨 필수 컬럼({required_cols})이 DF에 없습니다.")
            return pd.DataFrame()

        df_sorted = df.sort_values(by=sort_col, ascending=False)
        df_top20 = df_sorted.head(20).copy() 
        
        # 최종 컬럼 선택 및 이름 변경
        df_final = df_top20[required_cols].rename(
            columns={sort_col: '순매수_거래대금'}
        )
        
        return df_final
