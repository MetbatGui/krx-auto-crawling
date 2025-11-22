from typing import Optional
from core.services.krx_fetch_service import KrxFetchService
from core.ports.daily_report_port import DailyReportPort
from core.ports.master_report_port import MasterReportPort
from core.ports.ranking_report_port import RankingReportPort
from core.ports.watchlist_port import WatchlistPort

class DailyRoutineService:
    """
    일일 크롤링 및 리포트 업데이트 루틴을 총괄하는 오케스트레이션 서비스.
    """

    def __init__(
        self,
        fetch_service: KrxFetchService,
        daily_port: DailyReportPort,
        master_port: MasterReportPort,
        ranking_port: RankingReportPort,
        watchlist_port: WatchlistPort
    ):
        self.fetch_service = fetch_service
        self.daily_port = daily_port
        self.master_port = master_port
        self.ranking_port = ranking_port
        self.watchlist_port = watchlist_port

    def execute(self, date_str: Optional[str] = None):
        """
        전체 일일 루틴을 실행합니다.
        1. 데이터 수집 (FetchService)
        2. 일별 리포트 저장 (DailyReportPort)
        3. 마스터 리포트 업데이트 (MasterReportPort)
        4. 수급 순위표 업데이트 (RankingReportPort)
        5. 관심종목 파일 저장 (WatchlistPort)
        """
        print(f"\n=== [DailyRoutineService] 루틴 시작 (Date: {date_str}) ===")

        # 1. 데이터 수집
        data_list = self.fetch_service.fetch_all_data(date_str)
        
        if not data_list:
            print("=== [DailyRoutineService] 🚨 수집된 데이터가 없습니다. 루틴을 종료합니다. ===")
            return

        print(f"\n=== [DailyRoutineService] 데이터 수집 완료 ({len(data_list)}건). 리포트 작업 시작... ===")

        # 2. 일별 리포트 저장
        print("\n--- [Step 1] 일별 리포트 저장 ---")
        self.daily_port.save_daily_reports(data_list)

        # 3. 마스터 리포트 업데이트
        print("\n--- [Step 2] 마스터 리포트 업데이트 ---")
        self.master_port.update_master_reports(data_list)

        # 4. 수급 순위표 업데이트
        print("\n--- [Step 3] 수급 순위표 업데이트 ---")
        self.ranking_port.update_ranking_report(data_list)

        # 5. 관심종목 파일 저장
        print("\n--- [Step 4] 관심종목 파일 저장 ---")
        self.watchlist_port.save_watchlist(data_list)

        print("\n=== [DailyRoutineService] 모든 루틴이 완료되었습니다. ===")
