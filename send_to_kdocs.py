import csv
import requests
import sys
import os
from typing import List

# ====================== 환경변수 ======================
TOKEN = os.environ.get("KDOCS_TOKEN")
TARGET_FILE_ID = os.environ.get("KDOCS_TARGET_FILE_ID")
SCRIPT_NAME = os.environ.get("KDOCS_SCRIPT_NAME")
MODE = os.environ.get("KDOCS_MODE", "sheet").lower()   # "sheet" 또는 "db"

CSV_FILE = "result.csv"

if not all([TOKEN, TARGET_FILE_ID, SCRIPT_NAME]):
    print("❌ 에러: KDOCS_TOKEN, KDOCS_TARGET_FILE_ID, KDOCS_SCRIPT_NAME 중 하나가 누락되었습니다.")
    sys.exit(1)

print(f"🔧 실행 모드: {MODE.upper()} | Target File ID: {TARGET_FILE_ID} | Script: {SCRIPT_NAME}")

# ====================== CSV 읽기 ======================
try:
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        data: List[List[str]] = list(reader)

    total_rows = len(data)
    print(f"📊 CSV 읽기 완료: 총 {total_rows}행 (헤더 포함)")

    if total_rows == 0:
        print("⚠️ CSV가 비어 있습니다.")
        sys.exit(1)

    # DB 모드에서도 헤더 포함해서 전체 전송
    if MODE == "db":
        print(f"   → DB 모드: 헤더 포함 {total_rows}행 전체 전송")
        print(f"   → 헤더 예시: {data[0][:10]}...")   # 디버깅용
    else:
        print(f"   → 일반 시트 모드: 헤더 포함 {total_rows}행 전체 전송")

    data_to_send = data

except Exception as e:
    print(f"❌ CSV 읽기 실패: {e}")
    sys.exit(1)

# ====================== Payload 구성 ======================
argv_key = "records" if MODE == "db" else "rows"

payload = {
    "Context": {
        "argv": {
            argv_key: data_to_send
        }
    }
}

# ====================== API 호출 ======================
API_URL = f"https://www.kdocs.cn/api/v3/ide/file/{TARGET_FILE_ID}/script/{SCRIPT_NAME}/sync_task"

headers = {
    "Content-Type": "application/json",
    "AirScript-Token": TOKEN
}

print(f"🚀 KDocs 전송 시작 → {len(data_to_send)}행 데이터 ({argv_key} 키 사용, 헤더 포함)")

try:
    response = requests.post(API_URL, headers=headers, json=payload, timeout=300)
    
    print(f"📡 Status Code: {response.status_code}")
    response_text = response.text
    print(f"📩 Response (첫 800자): {response_text[:800]}")

    if response.status_code == 200:
        try:
            resp_json = response.json()
            if (resp_json.get("success") is True or 
                resp_json.get("status") == "success" or 
                resp_json.get("code") == 0 or 
                "ok" in str(resp_json).lower()):
                print("✅ KDocs 업데이트 성공!")
            else:
                print("⚠️ AirScript 응답이 실패로 보입니다:")
                print(resp_json)
                sys.exit(1)
        except:
            print("✅ 상태코드 200 (JSON 파싱 실패 → 성공으로 간주)")
    else:
        print("⛔ KDocs 업데이트 실패!")
        sys.exit(1)

except requests.exceptions.Timeout:
    print("❌ 타임아웃 발생")
    sys.exit(1)
except Exception as e:
    print(f"❌ 요청 중 에러: {e}")
    sys.exit(1)
