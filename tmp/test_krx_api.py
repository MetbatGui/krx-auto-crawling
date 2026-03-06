import requests

# KRX 정보데이터시스템의 OTP(및 API 조회)는 기본적으로 Session 쿠키가 없으면 거절(LOGOUT 리턴)합니다.
# KrxHttpAdapter의 _login은 로그인을 수행하지만,
# 로그인 없이도 쿠키(mdc.client_session=true)를 발급받을 수 있습니다.

session = requests.Session()
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
session.headers.update({"User-Agent": user_agent})

print("1. 세션 초기화 접근")
# 통계 사이트 초기 접근 (JSESSIONID 발급 및 mdc.client_session 세팅 목적)
session.get("https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201", timeout=10)

print(f"현재 쿠키: {session.cookies.get_dict()}")

url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': "https://data.krx.co.kr",
    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
    'X-Requested-With': 'XMLHttpRequest'
}
payload = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
    'locale': 'ko_KR',
    'mktId': 'ALL',
    'trdDd': '20250228',
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false',
}

print("2. 마켓 데이터 조회 요청")
res = session.post(url, headers=headers, data=payload)
print(f"Status Code: {res.status_code}")
if res.status_code == 200:
    data = res.json()
    items = data.get('OutBlock_1', []) or data.get('output', [])
    print(f"Items fetched: {len(items)}")
    for item in items[:2]:
        print(item)
else:
    print(res.text)
