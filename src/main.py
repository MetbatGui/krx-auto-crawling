import datetime
import os
import argparse
import sys
from dotenv import load_dotenv

# Services
from core.services.daily_routine_service import DailyRoutineService
from core.services.krx_fetch_service import KrxFetchService

# Adapters
from infra.adapters.storage import LocalStorageAdapter
from infra.adapters.krx_http_adapter import KrxHttpAdapter
from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from infra.adapters.master_excel_adapter import MasterExcelAdapter
from infra.adapters.ranking_excel_adapter import RankingExcelAdapter
from infra.adapters.watchlist_file_adapter import WatchlistFileAdapter

def parse_arguments():
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(description='KRX Auto Crawling Service')
    parser.add_argument(
        'date', 
        nargs='?', 
        help='Target date in YYYYMMDD format (default: today)',
        default=None
    )
    return parser.parse_args()

def main():
    """
    KRX 자동 크롤링 프로젝트의 메인 진입점.
    의존성을 주입하고 DailyRoutineService를 실행합니다.
    """
    # 1. 환경 변수 로드
    load_dotenv()
    
    # 2. CLI 인자 처리
    args = parse_arguments()
    
    if args.date:
        target_date = args.date
        # 간단한 날짜 형식 검증
        if len(target_date) != 8 or not target_date.isdigit():
            print(f"🚨 [Main] Invalid date format: {target_date}. Please use YYYYMMDD.")
            sys.exit(1)
    else:
        target_date = datetime.date.today().strftime('%Y%m%d')

    # 3. 기본 경로 설정
    BASE_OUTPUT_PATH = "output"
    
    print(f"--- [Main] KRX Auto Crawling System Initializing (Target: {target_date}) ---")

    # 4. StoragePort 인스턴스 생성
    storage = LocalStorageAdapter(base_path=BASE_OUTPUT_PATH)

    # 5. 어댑터(Adapters) 인스턴스 생성 및 의존성 주입
    # (Infra Layer)
    krx_adapter = KrxHttpAdapter()
    daily_adapter = DailyExcelAdapter(storage=storage)
    master_adapter = MasterExcelAdapter(base_path=BASE_OUTPUT_PATH)  # 아직 미마이그레이션
    ranking_adapter = RankingExcelAdapter(base_path=BASE_OUTPUT_PATH, file_name="2025일별수급순위정리표.xlsx")  # 아직 미마이그레이션
    watchlist_adapter = WatchlistFileAdapter(storage=storage)

    # 6. 서비스(Services) 인스턴스 생성 및 의존성 주입
    # (Core Layer)
    fetch_service = KrxFetchService(krx_port=krx_adapter)
    
    routine_service = DailyRoutineService(
        fetch_service=fetch_service,
        daily_port=daily_adapter,
        master_port=master_adapter,
        ranking_port=ranking_adapter,
        watchlist_port=watchlist_adapter
    )

    # 6. 메인 루틴 실행
    try:
        routine_service.execute(date_str=target_date)
    except Exception as e:
        print(f"\n🚨 [Main] Critical Error during execution: {e}")

if __name__ == '__main__':
    main()
