import os
from dotenv import load_dotenv
import dart_fss as dart

# --- 1. .env 로드 및 API 키 설정 (기존 코드와 동일) ---
load_dotenv()
API_KEY = os.environ.get("DART_API_KEY")

if API_KEY is None:
    print("오류: DART_API_KEY가 환경 변수에 설정되지 않았습니다.")
    print("   .env 파일을 확인하거나 Github Secrets 설정을 확인하세요.")
    exit()

try:
    dart.set_api_key(api_key=API_KEY)
    print("DART API 키 설정 완료.")
except Exception as e:
    print(f"DART API 키 설정 오류: {e}")
    exit()

# --- 2. DART 기업 목록 로드 (기존 코드와 동일) ---
try:
    print("DART 전체 기업 목록을 로드/업데이트합니다...")
    corp_list = dart.get_corp_list()
    print("기업 목록 로드 완료.")
except Exception as e:
    print(f"DART 기업 목록 로드 중 오류 발생: {e}")
    exit()

# --- 3. 수집 대상 기업 이름 (기존 코드와 동일) ---
TEST_STOCK_NAMES = ["삼성전자", "LG에너지솔루션", "SK하이닉스", "3S"] # 오타 수정: LG에너지솔루션

def get_corp_codes_from_names(stock_names, corp_list_obj):
    """
    회사 이름 리스트를 받아 DART 고유번호 딕셔너리로 변환합니다.
    (기존 코드와 동일)
    """
    print(f"\n--- 총 {len(stock_names)}개 기업 고유번호 변환 시작 ---")
    
    code_map = {}
    for name in stock_names:
        try:
            corp_search_result = corp_list_obj.find_by_corp_name(name, exactly=True)
            
            if corp_search_result:
                corp_obj = corp_search_result[0] 
                code_map[name] = corp_obj.corp_code
                print(f"  [성공] {name: <10} -> {corp_obj.corp_code}")
            else:
                code_map[name] = None
                print(f"  [실패] {name: <10} -> DART 목록에서 찾을 수 없음")
                
        except Exception as e:
            code_map[name] = None
            print(f"  [오류] {name: <10} -> 검색 중 오류 발생: {e}")
            
    return code_map


if __name__ == "__main__":
    
    # 1. 기업 이름 리스트 -> {이름: 고유번호} 딕셔너리로 변환
    corp_code_dict = get_corp_codes_from_names(TEST_STOCK_NAMES, corp_list)
    
    # --- [수정 및 추가된 부분] ---
    
    target_corp_name = "삼성전자"
    target_corp_code = corp_code_dict.get(target_corp_name)
    
    # 2. 삼성전자 고유번호가 유효한지 확인
    if target_corp_code:
        print(f"\n--- '{target_corp_name}' (코드: {target_corp_code}) 재무제표 추출 시작 ---")
        
        # 3. 저장 경로 설정 및 폴더 생성 (os.makedirs 사용)
        save_directory = "output/분기별_실적_수집"
        os.makedirs(save_directory, exist_ok=True)
        
        # 4. 재무제표 추출
        try:
            # report_tp='quarter' : 분기 + 반기 + 연간 보고서를 모두 가져옴
            # bgn_de='20200101' : 2020년부터의 데이터를 가져옴 (너무 길면 오래 걸림)
            fs = dart.fs.extract(corp_code=target_corp_code, 
                                 bgn_de='20200101', 
                                 report_tp='quarter')
            
            # 5. FinancialStatement 객체의 .save() 메소드로 엑셀 저장
            # (path에 지정한 폴더로 저장됩니다)
            output_filename = f"{target_corp_name}_재무제표.xlsx"
            fs.save(filename=output_filename, path=save_directory)
            
            print(f"\n[성공] 재무제표를 엑셀 파일로 저장했습니다.")
            print(f" -> 저장 위치: {os.path.join(save_directory, output_filename)}")

        except Exception as e:
            print(f"\n[오류] '{target_corp_name}' 재무제표 추출 또는 저장 중 오류 발생: {e}")
            print("      (데이터가 없거나 API 요청에 문제가 있을 수 있습니다)")

    else:
        print(f"\n[실패] '{target_corp_name}'의 고유번호를 찾을 수 없습니다.")
        
    print("\n--- 스크립트 실행 완료 ---")