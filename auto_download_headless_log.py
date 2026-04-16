from __future__ import annotations

# ===== Imports =====
import os
import sys
import time
import glob
import re
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from google.cloud import bigquery

# ===== Stdout to log.txt =====
class DualLogger:
    def __init__(self, filepath: str):
        self.terminal = sys.__stdout__
        self.log = open(filepath, "w", encoding="utf-8")

    def write(self, message: str):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = sys.stderr = DualLogger("log.txt")

# ===== Environment / Settings =====
RUNNER = os.getenv("GITHUB_ACTIONS") == "true"

# BigQuery
PROJECT_ID = os.getenv("GCP_PROJECT") or "savvy-mantis-457008-k6"
DATASET_ID = os.getenv("BQ_DATASET") or "raw_data"
TABLE_ID = os.getenv("BQ_TABLE") or "goods_csv"

# Login
LOGIN_ID = os.getenv("LOGIN_ID") or "ppazic"
LOGIN_PW = os.getenv("LOGIN_PW") or "123123"

# Download folder
if RUNNER:
    downloads_folder = str((Path.cwd() / "downloads").resolve())
else:
    downloads_folder = r"C:\Users\white\Downloads\csv"
Path(downloads_folder).mkdir(parents=True, exist_ok=True)

# GCP creds path
GOOGLE_CREDS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str((Path(__file__).parent / "bigquery-credentials.json").resolve()),
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDS

# URLs
LOGIN_URL = "https://silkroad21.co.kr/pzadm/Login.asp"
LIST_URL = "https://silkroad21.co.kr/Admin/Acting/Acting_S.asp?gMnu1=101&gMnu2=10101"

# ===== Helpers =====
def accept_alert_safe(driver, timeout: int = 3) -> bool:
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        print("[ALERT]", alert.text)
        alert.accept()
        return True
    except Exception:
        return False


def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": downloads_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "download.extensions_to_open": "",
        },
    )

    chrome_bin = os.getenv("CHROME_PATH")
    if chrome_bin:
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)

    # 허용 가능한 곳에서 다운로드 허용
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": downloads_folder},
        )
    except Exception:
        pass

    driver.implicitly_wait(5)
    return driver


def do_login(driver: webdriver.Chrome) -> None:
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)

    id_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemId")))
    pw_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemPw")))

    for el, val in ((id_el, LOGIN_ID), (pw_el, LOGIN_PW)):
        try:
            el.clear()
        except Exception:
            pass
        el.send_keys(val)
    pw_el.send_keys(Keys.RETURN)

    # 알럿 한 번 처리 후 재시도
    if accept_alert_safe(driver, timeout=3):
        id_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "sMemId")))
        pw_el = driver.find_element(By.NAME, "sMemPw")
        id_el.clear()
        id_el.send_keys(LOGIN_ID)
        pw_el.clear()
        pw_el.send_keys(LOGIN_PW)
        pw_el.send_keys(Keys.RETURN)
        accept_alert_safe(driver, timeout=2)

    # Login.asp에서 벗어나면 성공
    try:
        wait.until(lambda d: "Login.asp" not in d.current_url)
    except TimeoutException:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            btn.click()
            WebDriverWait(driver, 10).until(lambda d: "Login.asp" not in d.current_url)
        except Exception:
            print("[LOGIN DEBUG] title =", driver.title)
            print("[LOGIN DEBUG] url   =", driver.current_url)
            raise RuntimeError("로그인에 실패했습니다. 계정/셀렉터 확인 필요")


def goto_with_auth(driver: webdriver.Chrome, url: str, login_hint: str = "Login.asp") -> None:
    driver.get(url)
    time.sleep(0.5)
    if login_hint in driver.current_url:
        print("[INFO] 세션 만료로 재로그인 시도")
        do_login(driver)
        driver.get(url)


def wait_for_download_complete(dirpath: str, timeout: int = 1000) -> None:
    end = time.time() + timeout
    pattern_cr = os.path.join(dirpath, "*.crdownload")
    pattern_csv = os.path.join(dirpath, "*.csv")

    while time.time() < end:
        if glob.glob(pattern_cr):
            time.sleep(0.8)
            continue
        if glob.glob(pattern_csv):
            return
        time.sleep(0.8)
    raise TimeoutError("다운로드 완료 대기 시간 초과")


# ===== Main =====
driver = make_driver(headless=True)
driver.command_executor.set_timeout(1800)   # ChromeDriver와의 통신 타임아웃 (10분)
driver.set_script_timeout(1800)             # JS 실행 시간 제한
driver.set_page_load_timeout(1800)          # 페이지 로딩 시간 제한
try:
    do_login(driver)
    goto_with_auth(driver, LIST_URL)

    # ===== 엑셀 다운로드 버튼 클릭 방식으로 변경 =====
    try:
        print("[INFO] 엑셀 다운로드 버튼 찾는 중...")
        wait = WebDriverWait(driver, 20)

        # onclick 또는 href 안에 fnPageExl('X14') 가 들어간 a 태그를 찾는다
        export_btn = wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "a[onclick*=\"fnPageExl('X14')\"], a[href*=\"fnPageExl('X14')\"]",
                )
            )
        )
        print("[INFO] 엑셀 다운로드 버튼 클릭")
        export_btn.click()
    except Exception as e:
        print("[WARN] 버튼 클릭 방식 실패, execute_script로 대체 시도:", e)
        # fallback: 그래도 안 되면 짧은 타임아웃으로 한번만 JS 직접 호출
        driver.set_script_timeout(10)
        driver.execute_script("fnPageExl('X14');")

    # 다운로드 과정에서 alert 뜨면 처리
    accept_alert_safe(driver, timeout=5)

    # CSV 생성/다운로드 완료 대기
    wait_for_download_complete(downloads_folder, timeout=1000)

finally:
    try:
        driver.quit()
    except Exception:
        pass

# 최신 CSV만 남기기
csv_files = glob.glob(os.path.join(downloads_folder, "*.csv"))
if not csv_files:
    print("❌ CSV 파일이 존재하지 않습니다. (다운로드 실패)")
    sys.exit(1)

latest_file = max(csv_files, key=os.path.getctime)
for fp in list(csv_files):
    if fp != latest_file:
        try:
            os.remove(fp)
            print("🗑 삭제됨:", os.path.basename(fp))
        except Exception:
            pass

# CSV 로딩
try:
    df = pd.read_csv(latest_file, encoding="utf-8-sig", dtype=str, on_bad_lines="skip")
except Exception:
    df = pd.read_csv(latest_file, encoding="cp949", dtype=str, on_bad_lines="skip")

print(f"📊 데이터 로딩 완료: {len(df)} rows")


def sanitize_columns(cols):
    seen = {}
    out = []
    for c in cols:
        c = (c or "").strip()
        c = re.sub(r"[^\w]", "_", c)
        if re.match(r"^\d", c):
            c = "_" + c
        base = c
        i = 1
        while c in seen:
            c = f"{base}_{i}"
            i += 1
        seen[c] = True
        out.append(c)
    return out


df.columns = sanitize_columns(df.columns)
df = df.dropna(how="all").drop_duplicates()
print("🧹 데이터 정제 완료")

# BigQuery 업로드
client = bigquery.Client(project=PROJECT_ID)
full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
job = client.load_table_from_dataframe(
    df,
    full_table_id,
    location="asia-northeast3",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
)
job.result()
print(f"✅ BigQuery 업로드 성공: {len(df)}건 → {full_table_id}")

# =====================================================================
# 🚀 [추가] KDocs DBSheet 듀얼 전송 파이프라인
# =====================================================================
import requests
import json

print("🚀 KDocs DBSheet로 데이터 전송 시작...")

# 1. URL과 토큰을 직접 문자열로 기재합니다.
KDOCS_WEBHOOK_URL = "https://www.kdocs.cn/api/v3/ide/file/cnIgZYoMts1i/script/V2-5vnUpdVQGXoWN9loeiAx39/sync_task"

# 💡 [수정 필수] KDocs 웹훅 복사 시 나왔던 AirScript-Token 값을 아래에 입력하세요.
AIRSCRIPT_TOKEN = "1Vg353OyhzW3n27xfSZKUh"

# 2. 헤더에 토큰 인증 정보 추가
headers = {
    "Content-Type": "application/json",
    "AirScript-Token": AIRSCRIPT_TOKEN
}

# 3. 데이터 전처리 (판다스의 NaN 값을 빈 문자열로 치환)
df_kdocs = df.fillna("")
all_data = df_kdocs.values.tolist()

# 4. KDocs 서버 과부하 및 403 차단 방지를 위해 100건씩 분할 전송
chunk_size = 100
total_chunks = (len(all_data) // chunk_size) + 1

for i in range(0, len(all_data), chunk_size):
    chunk = all_data[i : i + chunk_size]
    payload = {"rows": chunk}

    try:
        response = requests.post(
            KDOCS_WEBHOOK_URL,
            json=payload,
            headers=headers,
            timeout=30
        )
        current_chunk = (i // chunk_size) + 1
        print(f"📡 [{current_chunk}/{total_chunks}회차] 응답 코드: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ 에러 내용: {response.text}")
            
    except Exception as e:
        print(f"❌ 전송 중 통신 에러 발생: {e}")

print("✅ KDocs DBSheet 릴레이 전송 프로세스 종료")
