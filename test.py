import requests

# 1. 테스트할 웹훅 주소
KDOCS_URL = "https://www.kdocs.cn/api/v3/ide/file/cnIgZYoMts1i/script/V2-5vnUpdVQGXoWN9loeiAx39/sync_task"

# 💡 [수정 필수] 발급받으신 실제 토큰값을 넣어주세요.
AIRSCRIPT_TOKEN = "1Vg353OyhzW3n27xfSZKUh"

headers = {
    "Content-Type": "application/json",
    "AirScript-Token": AIRSCRIPT_TOKEN
}

# 2. 테스트용 가짜 데이터 1줄 (주문번호, 고객명, 상품명, 수량, 결제금액, 상태, 주문일시)
# 첫 번째 값이 A열(주문번호)에 해당합니다.
dummy_data = [
    ["TEST-9999", "테스트고객", "샘플상품", 1, 10000, "입고완료", "2026-04-16"]
]

payload = {"rows": dummy_data}

print("🚀 KDocs 에어스크립트 응답 테스트 시작...")

try:
    # 3. KDocs로 쏘고 결과 받기
    res = requests.post(KDOCS_URL, json=payload, headers=headers, timeout=10)
    
    print(f"📡 상태 코드: {res.status_code}")
    print(f"📩 KDocs 진짜 응답 내용: {res.text}")  # 👈 이 부분이 에러 해결의 핵심입니다!

except Exception as e:
    print(f"❌ 전송 중 통신 에러 발생: {e}")