from typing import Optional
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.ports.watchlist_port import WatchlistPort
from core.logger import logger


class DailyRoutineService:
    """일일 크롤링 및 리포트 업데이트 루틴을 총괄하는 오케스트레이션 서비스.

    Attributes:
        fetch_service (KrxFetchService): KRX 데이터 수집 서비스.
        master_port (MasterReportService): 마스터 리포트 서비스.
        ranking_port (RankingAnalysisService): 순위 분석 서비스.
        watchlist_port (WatchlistPort): 관심종목 저장 포트.
    """

    def __init__(
        self,
        fetch_service: KrxFetchService,
        master_port: MasterReportService,
        ranking_port: RankingAnalysisService,
        watchlist_port: WatchlistPort
    ):
        """DailyRoutineService 초기화.

        Args:
            fetch_service (KrxFetchService): KRX 데이터 수집 서비스.
            master_port (MasterReportService): 마스터 리포트 서비스.
            ranking_port (RankingAnalysisService): 순위 분석 서비스.
            watchlist_port (WatchlistPort): 관심종목 저장 포트.
        """
        self.fetch_service = fetch_service
        self.master_port = master_port
        self.ranking_port = ranking_port
        self.watchlist_port = watchlist_port

    def execute(self, date_str: Optional[str] = None, force_fetch: bool = False):
        """전체 일일 루틴을 실행합니다.

        다음 단계를 순차적으로 실행합니다:
        0. 데이터 확보 (웹 수집 진행)
        1. 일별 관심종목 파일 저장
        2. 마스터 리포트 업데이트
        3. 누적 상위종목 watchlist 저장
        4. 수급 순위표 업데이트

        Args:
            date_str (Optional[str]): 실행할 날짜 문자열 (YYYYMMDD). None일 경우 오늘 날짜를 사용합니다.
            force_fetch (bool): True일 경우 force_fetch 모드로 수집을 시작합니다.
        """
        import datetime
        if date_str is None:
            date_str = datetime.date.today().strftime('%Y%m%d')

        logger.info(f"[DailyRoutineService] 루틴 시작 (Date: {date_str})")

        # Step 0: 데이터 확보
        if force_fetch:
            logger.info("[DailyRoutineService] 강제 수집 모드. KRX 수집을 시작합니다.")
        else:
            logger.info("[DailyRoutineService] KRX 웹 수집을 시작합니다.")
        
        data_list = self.fetch_service.fetch_all_data(date_str)
        
        if not data_list:
            logger.error("[DailyRoutineService] 데이터 확보 실패 (수집 불가). 루틴을 종료합니다.")
            return

        logger.info(f"[DailyRoutineService] 데이터 확보 완료 ({len(data_list)}건). 리포트 작업 시작")

        logger.info("[DailyRoutineService] [Step 1] 일별 관심종목 파일 저장")
        self.watchlist_port.save_watchlist(data_list)

        logger.info("[DailyRoutineService] [Step 2] 마스터 리포트 업데이트")
        # Master Report Update
        top_stocks_map = self.master_port.update_reports(data_list)

        logger.info("[DailyRoutineService] [Step 3] 누적 상위종목 watchlist 저장")
        if top_stocks_map:
            self.watchlist_port.save_cumulative_watchlist(top_stocks_map, date_str)
        else:
            logger.warning("[DailyRoutineService] 누적 상위종목 데이터가 없습니다")

        logger.info("[DailyRoutineService] [Step 4] 수급 순위표 업데이트")
        self.ranking_port.update_ranking_report(data_list)

        logger.info("[DailyRoutineService] 모든 루틴이 완료되었습니다.")
