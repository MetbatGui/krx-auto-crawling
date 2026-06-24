"""순위표 생성 및 분석 오케스트레이션 서비스"""

import datetime
from typing import List

from core.domain.models import KrxData
from core.services.ranking_data_service import RankingDataService
from core.ports.ranking_report_port import RankingReportPort


class RankingAnalysisService:
    """순위표 워크플로우를 조율하는 오케스트레이션 서비스.

    비즈니스 로직(RankingDataService)과 리포트 생성(RankingReportPort)을
    조합하여 전체 순위표 업데이트 프로세스를 관리합니다.

    Attributes:
        data_service (RankingDataService): 순위 데이터 분석 서비스
        report_port (RankingReportPort): 리포트 생성 포트
    """
    
    def __init__(
        self,
        data_service: RankingDataService,
        report_port: RankingReportPort
    ):
        """RankingAnalysisService 초기화.

        Args:
            data_service: 순위 데이터 분석 서비스
            report_port: 리포트 생성 포트 (Excel, PDF 등)
        """
        self.data_service = data_service
        self.report_port = report_port
        print("[Service:RankingAnalysis] 초기화 완료")
    
    def update_ranking_report(self, data_list: List[KrxData]) -> None:
        """순위표 전체 업데이트 워크플로우를 실행합니다.
        
        다음 단계를 수행합니다:
        1. 데이터 검증
        2. 공통 종목 계산 (비즈니스 로직)
        3. 리포트 생성 (Port에 위임)
        
        Args:
            data_list (List[KrxData]): 업데이트할 KRX 데이터 리스트.
        """
        if not self.data_service.validate_data(data_list):
            return
        
        data_map = self._build_data_map(data_list)
        report_date = self._extract_date(data_list[0].date_str)
        common_stocks = self.data_service.calculate_common_stocks(data_map)
        
        self._execute_report_update(report_date, data_map, common_stocks)
    
    def _build_data_map(self, data_list: List[KrxData]) -> dict:
        """데이터 리스트를 딕셔너리로 변환합니다."""
        return {item.key: item.data for item in data_list if not item.data.empty}
    
    def _extract_date(self, date_str: str) -> datetime.date:
        """날짜 문자열을 date 객체로 변환합니다."""
        return datetime.datetime.strptime(date_str, '%Y%m%d').date()
    
    def _execute_report_update(
        self,
        report_date: datetime.date,
        data_map: dict,
        common_stocks: dict
    ):
        """리포트 업데이트를 실행합니다."""
        print(f"    -> [Service:RankingAnalysis] 순위표 업데이트 시작...")
        
        success = self.report_port.update_report(report_date, data_map, common_stocks)
        
        status = "[OK] 순위표 업데이트 완료" if success else "[Error] 순위표 업데이트 실패"
        print(f"    -> [Service:RankingAnalysis] {status}")
