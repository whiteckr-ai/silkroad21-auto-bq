import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import urllib3
import json
import time

# SSL 인증서 경고 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_customs_rate(max_retries=3):
    service_key = "2758a1afe287a2143a6893f6a4d637788f34421745d71f6a5ef93d82ae20f114"
    today = datetime.now().strftime('%Y%m%d')
    url = "https://apis.data.go.kr/1220000/retrieveTrifFxrtInfo/getRetrieveTrifFxrtInfo"
    params = {
        'serviceKey': service_key,
        'aplyBgnDt': today,
        'weekFxrtTpcd': '2'
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, verify=False, timeout=15)
            response.encoding = 'utf-8'

            # ⭐ 진단용: 실제 응답을 그대로 출력해서 원인을 확인
            print(f"[디버그] Status: {response.status_code}")
            print(f"[디버그] Response (첫 500자): {response.text[:500]}")

            root = ET.fromstring(response.content)

            result_code = root.findtext('.//resultCode')
            result_msg = root.findtext('.//resultMsg')

            if result_code is not None and result_code != '00':
                print(f"⚠️ [시도 {attempt}/{max_retries}] API 응답 에러: {result_code} - {result_msg}")
                if attempt < max_retries:
                    time.sleep(5)
                continue

            for item in root.findall('.//item'):
                curr = item.find('currSgn')
                if curr is not None and curr.text == 'CNY':
                    rate = item.find('fxrt').text
                    print(f"✅ 관세청 환율 조회 성공 (시도 {attempt}/{max_retries}): {rate}")
                    return rate

            print(f"⚠️ [시도 {attempt}/{max_retries}] 위안화 환율 데이터를 찾을 수 없습니다.")

        except ET.ParseError as e:
            print(f"⚠️ [시도 {attempt}/{max_retries}] XML 파싱 실패: {e}")
            print(f"[디버그] 원본 응답: {response.text[:500] if 'response' in dir() else '응답 없음'}")
        except Exception as e:
            print(f"⚠️ [시도 {attempt}/{max_retries}] 관세청 API 호출 에러: {e}")

        if attempt < max_retries:
            time.sleep(5)

    print("❌ 관세청 API 최종 실패 (재시도 모두 소진)")
    return None


def get_krw_rate():
    # silkroad21 서버가 뿌리는 텍스트 값(예: "241")을 그대로 읽어온다.
    url = "https://silkroad21.co.kr/krw_rate.txt"
    try:
        response = requests.get(url, verify=False, timeout=15)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            value = response.text.strip()
            print(f"✅ krw_rate.txt 수신: {value}")
            return value
        else:
            print(f"❌ krw_rate.txt 조회 실패: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ krw_rate.txt 호출 에러: {e}")
        return None


def send_to_kdocs(cny_rate, krw_rate):
    # CNY 환율이 없으면 절대 전송하지 않는다.
    # (전송해버리면 AirScript 쪽 테스트용 더미값(999.99 등)이나 빈 값으로
    #  시트가 덮어써질 위험이 있어, 실패 시엔 시트의 기존 값을 그대로 둔다.)
    if not cny_rate:
        print("❌ CNY 환율 조회 실패 — 이번 전송을 건너뜁니다. (시트의 기존 값 유지)")
        return

    if not krw_rate:
        print("⚠️ KRW 값 없음 — CNY만 전송합니다.")

    # raw data 파일의 환율 전용 스크립트로 전송
    webhook_url = "https://www.kdocs.cn/api/v3/ide/file/541329031118/script/V2-X5RimFUYaSqf8lgfdWIoX/sync_task"

    payload = {
        "Context": {
            "argv": {
                "cny_rate": cny_rate,
                "krw_rate": krw_rate
            }
        }
    }

    headers = {
        "Content-Type": "application/json",
        "AirScript-Token": "1Kg3yPgGLOWMmvuTc6eLdD"  # 💡 새 스크립트의 토큰으로 교체
    }

    try:
        response = requests.post(webhook_url, json=payload, headers=headers)
        if response.status_code == 200:
            print(f"✅ KDocs 웹훅 전송 성공! CNY: {cny_rate} / KRW: {krw_rate}")
        else:
            print(f"❌ KDocs 전송 실패: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ KDocs 연동 에러 발생: {e}")


if __name__ == "__main__":
    print("🔄 관세청 고시환율 및 krw_rate.txt 조회를 시작합니다...")
    cny_rate = get_customs_rate()
    krw_rate = get_krw_rate()
    print(f"수신 결과 -> CNY: {cny_rate}, KRW(krw_rate.txt): {krw_rate}")
    send_to_kdocs(cny_rate, krw_rate)
