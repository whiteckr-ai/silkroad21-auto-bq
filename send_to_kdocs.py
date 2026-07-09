import csv
import requests
import sys
import os
import time
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

# ====================== API 호출 (재시도 포함) ======================
API_URL = f"https://www.kdocs.cn/api/v3/ide/file/{TARGET_FILE_ID}/script/{SCRIPT_NAME}/sync_task"

headers = {
    "Content-Type": "application/json",
    "AirScript-Token": TOKEN
}

print(f"🚀 KDocs 전송 시작 → {len(data_to_send)}행 데이터 ({argv_key} 키 사용, 헤더 포함)")

MAX_RETRIES = 4
RETRY_WAIT_SECONDS = 20  # 시도마다 이 값 * 시도횟수 만큼 대기 (백오프: 20/40/60초)

success = False
last_status = None
last_text = ""

for attempt in range(1, MAX_RETRIES + 1):
    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=300)

        last_status = response.status_code
        last_text = response.text
        print(f"📡 [시도 {attempt}/{MAX_RETRIES}] Status Code: {last_status}")
        print(f"📩 Response (첫 800자): {last_text[:800]}")

        if response.status_code == 200:
            try:
                resp_json = response.json()

                top_status = resp_json.get("status")
                top_error = resp_json.get("error")
                data = resp_json.get("data")
                data_result = data.get("result") if isinstance(data, dict) else None

                is_success = (
                    # 실제 관찰된 성공 응답 형식: {"data": {..., "result": "Action Completed"},
                    #                             "error": "", "status": "finished"}
                    (top_status == "finished" and not top_error)
                    or data_result == "Action Completed"
                    or resp_json.get("success") is True
                    or resp_json.get("status") == "success"
                    or resp_json.get("code") == 0
                )

                if is_success:
                    print("✅ KDocs 업데이트 성공!")
                    success = True
                    break
                else:
                    print("⚠️ AirScript 응답이 실패로 보입니다:")
                    print(resp_json)
                    break  # 200인데 내용상 실패면 재시도 의미 없음, 바로 종료
            except Exception:
                print("✅ 상태코드 200 (JSON 파싱 실패 → 성공으로 간주)")
                success = True
                break

        resp_lower = last_text.lower()

        # ── 403 ScriptRetryLater: KDocs 쪽 rate-limit → 대기 후 재시도 ──────
        is_retry_later = (
            response.status_code == 403
            and "scriptretrylater" in resp_lower
        )

        # ── 타임아웃-추정 성공 처리 ──────────────────────────────────
        # KDocs AirScript는 대용량(4.5만 행 규모)을 한 번에 쓸 때 처리 시간이
        # 게이트웨이 응답 한도를 넘겨 500 + {"errno":10000,"result":"Unavailable"}
        # 를 반환하지만, 시트 쓰기 자체는 백그라운드에서 완료되는 것으로 관찰됨.
        # 따라서 이 특정 응답에 한해 '실패'가 아니라 '백그라운드 완료 추정'으로
        # 처리하여 종료 코드 0을 반환한다. 그 외의 500이나 다른 상태코드는
        # 기존대로 실패로 간주한다.
        # 주의: 이는 응답으로 확정된 성공이 아니라 관찰 기반 추정이므로,
        #       실제 반영 여부는 KDocs 시트에서 별도 확인이 필요할 수 있다.
        is_timeout_presumed = (
            response.status_code == 500
            and ("unavailable" in resp_lower or "10000" in resp_lower)
        )

        if is_timeout_presumed:
            print("⏳ KDocs 500(Unavailable) 수신 — 대용량 처리 타임아웃으로 판단.")
            print("   → 시트 쓰기는 백그라운드에서 완료된 것으로 추정하고 성공 처리합니다.")
            print("   → (확정된 성공 아님. 필요 시 KDocs 시트 행 수를 직접 확인하세요.)")
            success = True
            break

        if is_retry_later and attempt < MAX_RETRIES:
            wait_sec = RETRY_WAIT_SECONDS * attempt
            print(f"⏳ 403 ScriptRetryLater 수신 — {wait_sec}초 후 재시도합니다...")
            time.sleep(wait_sec)
            continue

        # 그 외 실패는 재시도 없이 종료
        print("⛔ KDocs 업데이트 실패!")
        break

    except requests.exceptions.Timeout:
        print(f"❌ [시도 {attempt}/{MAX_RETRIES}] 타임아웃 발생")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT_SECONDS * attempt)
            continue
        break
    except Exception as e:
        print(f"❌ [시도 {attempt}/{MAX_RETRIES}] 요청 중 에러: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT_SECONDS * attempt)
            continue
        break

if not success:
    print(f"🚨 최종 실패 (마지막 상태: {last_status})")
    sys.exit(1)
