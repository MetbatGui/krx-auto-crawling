import pandas as pd
import datetime
from typing import TypedDict, Dict, Any, Set
from core.ports.excel_ranking_report_port import ExcelRankingReportPort
from core.tasks.krx_net_value.standardize_data import StandardizeDataTaskOutput

# --- Task Input ---
# 이 Task는 'StandardizedDataOutput'에서 데이터를 가져옵니다.
class UpdateRankingReportTaskInput(StandardizeDataTaskOutput):
    """
    이 Task는 'standardized_data'와 'report_date_dt'가 필요합니다.
    """
    pass

# --- Task Output ---
class UpdateRankingReportTaskOutput(TypedDict):
    """
    일별 순위 리포트 업데이트 Task의 실행 결과.
    status: 'success', 'skipped', 'error'
    message: 실행 결과 메시지
    common_stocks_kospi: [디버깅용] 코스피 공통 항목 수
    common_stocks_kosdaq: [디버깅용] 코스닥 공통 항목 수
    """
    status: str
    message: str
    common_stocks_kospi: int
    common_stocks_kosdaq: int

# --- Task ---
class UpdateRankingReportTask:
    # ... (init, _find_common_stocks 메서드는 동일) ...

    def execute(
        self,
        context: Dict[str, Any] # TypedDict 대신 일반 Dict 사용 (report_date_dt 없을 수 있음)
    ) -> UpdateRankingReportTaskOutput:

        task_name = self.__class__.__name__
        print(f"--- 🚀 7. {task_name} 시작 ---")

        try:
            # 1. 컨텍스트에서 데이터 추출 및 날짜 변환 (수정된 부분)
            date_str: str = context.get('date_str')
            if not date_str:
                print(f"    -> [Task] 🚨 'date_str' 키가 context에 없습니다.")
                # (날짜 정보가 없으면 작업 불가)
                return {
                    'status': 'skipped',
                    'message': "'date_str'이 없어 날짜를 알 수 없습니다.",
                    'common_stocks_kospi': 0,
                    'common_stocks_kosdaq': 0
                }
            
            # [수정] date_str을 datetime.date 객체로 변환
            try:
                report_date: datetime.date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                print(f"    -> [Task] 🚨 'date_str'({date_str}) 형식이 잘못되었습니다 (YYYYMMDD 필요).")
                return {
                    'status': 'skipped',
                    'message': f"잘못된 날짜 형식: {date_str}",
                    'common_stocks_kospi': 0,
                    'common_stocks_kosdaq': 0
                }
            
            # --- (이후 로직은 report_date를 사용하므로 동일) ---
            
            all_data: Dict[str, pd.DataFrame] = context['standardized_data']
            
            # (데이터 무결성 검사)
            required_keys = ['KOSPI_foreigner', 'KOSPI_institutions', 'KOSDAQ_foreigner', 'KOSDAQ_institutions']
            if not all(key in all_data for key in required_keys):
                print(f"    -> [Task] 🚨 'standardized_data'에 필요한 키가 부족합니다.")
                return {
                    'status': 'skipped',
                    'message': "공통 항목 계산에 필요한 데이터가 부족합니다.",
                    'common_stocks_kospi': 0,
                    'common_stocks_kosdaq': 0
                }

            # 2. [Task 4] 공통 항목 계산
            common_kospi = self._find_common_stocks(
                all_data['KOSPI_foreigner'],
                all_data['KOSPI_institutions']
            )
            common_kosdaq = self._find_common_stocks(
                all_data['KOSDAQ_foreigner'],
                all_data['KOSDAQ_institutions']
            )
            
            common_stocks_map = {
                'KOSPI': common_kospi,
                'KOSDAQ': common_kosdaq
            }
            print(f"    -> [Task] 공통 항목 계산 완료 (KOSPI: {len(common_kospi)}개, KOSDAQ: {len(common_kosdaq)}개)")

            # 3. Port에 전달할 데이터 준비
            data_to_paste = {
                'KOSPI_foreigner': all_data['KOSPI_foreigner'],
                'KOSPI_institutions': all_data['KOSPI_institutions'],
                'KOSDAQ_foreigner': all_data['KOSDAQ_foreigner'],
                'KOSDAQ_institutions': all_data['KOSDAQ_institutions']
            }
            previous_date = report_date - datetime.timedelta(days=1)
            
            # 4. Adapter 호출
            print(f"    -> [Task] ExcelRankingReportPort 호출 (Date: {report_date.strftime('%Y-%m-%d')})...")
            success = self.report_port.update_ranking_report(
                report_date=report_date, # 변환된 객체 전달
                previous_date=previous_date,
                data_to_paste=data_to_paste,
                common_stocks=common_stocks_map
            )

            if not success:
                raise Exception("Adapter가 False를 반환 (엑셀 저장/수정 실패)")

            message = f"일별 수급 순위 정리표 업데이트 완료 (KOSPI 공통: {len(common_kospi)}개)"
            print(f"    -> [Task] ✅ {message}")
            
            return {
                'status': 'success',
                'message': message,
                'common_stocks_kospi': len(common_kospi),
                'common_stocks_kosdaq': len(common_kosdaq)
            }

        except Exception as e:
            error_msg = f"일별 수급 순위 정리표 업데이트 중 오류: {e}"
            print(f"    -> [Task] 🚨 {error_msg}")
            return {
                'status': 'error',
                'message': error_msg,
                'common_stocks_kospi': 0,
                'common_stocks_kosdaq': 0
            }