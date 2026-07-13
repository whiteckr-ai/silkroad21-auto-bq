import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import urllib3
import json

# SSL 인증서 경고 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_customs_rate():
    # 💡 발급받으신 인증키를 여기에 다시 입력해 주세요
    api_key = "k250k246v024z146n060b070c0"
    today = datetime.now().strftime('%Y%m%d')
    url = "https://unipass.customs.go.kr:38010/ext/rest/trifFxrtInfoQry/retrieveTrifFxrtInfo"

    params = {
        'crkyCn': api_key,
        'qryYymmDd': today,
        'imexTp': '2'  # 수입
    }

    try:
        response = requests.get(url, params=params, verify=False, timeout=15)
        response.encoding = 'utf-8'
        root = ET.fromstring(response.content)

        # XML에서 위안화(CNY) 환율만 추출
        for item in root.findall('.//trifFxrtInfoQryRsltVo'):
            curr = item.find('currSgn')
            if curr is not None and curr.text == 'CNY':
                return item.find('fxrt').text

        print("❌ 위안화 환율 데이터를 찾을 수 없습니다.")
        return None

    except Exception as e:
        print(f"❌ 관세청 API 호출 에러: {e}")
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
    # 둘 다 없으면 보낼 게 없음
    if not cny_rate and not krw_rate:
        print("전송할 데이터가 없습니다.")
        return

    # 대표님이 제공해주신 KDocs 웹훅 URL
    webhook_url = "https://www.kdocs.cn/api/v3/ide/file/cpNtCZyCV88I/script/V2-pgD1PCjAOrmRh8WkVvZx8/sync_task"

    # KDocs로 보낼 데이터 구조 (JSON)
    #  - cny_rate : 관세청 위안화 고시환율
    #  - krw_rate : silkroad21 krw_rate.txt 값
    #  ※ KDocs AirScript 쪽에서 argv.krw_rate 를 읽어 셀에 쓰도록 함께 수정해야 반영됨.
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
        "AirScript-Token": "1Kg3yPgGLOWMmvuTc6eLdD"  # 💡 이 줄이 반드시 추가되어야 합니다.
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
