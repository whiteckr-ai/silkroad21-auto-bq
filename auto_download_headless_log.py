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

    if accept_alert_safe(driver, timeout=3):
        id_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "sMemId")))
        pw_el = driver.find_element(By.NAME, "sMemPw")
        id_el.clear()
        id_el.send_keys(LOGIN_ID)
        pw_el.clear()
        pw_el.send_keys(LOGIN_PW)
        pw_el.send_keys(Keys.RETURN)
        accept_alert_safe(driver, timeout=2)

    try:
        wait.until(lambda d: "Login.asp" not in d.current_url)
    except TimeoutException:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            btn.click()
            WebDriverWait(driver, 10).until(lambda d: "Login.asp" not in d.current_url)
        except Exception:
            raise RuntimeError("로그인에 실패했습니다. 계정/셀렉터 확인 필요")

def goto_with_auth(driver: webdriver.Chrome, url: str, login_hint: str = "Login.asp") -> None:
    driver.get(url)
    time.sleep(0.5)
    if login_hint in driver.current_url:
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
driver.command_executor.set_timeout(1800)
driver.set_script_timeout(1800)
driver.set_page_load_timeout(1800)
try:
    do_login(driver)
    goto_with_auth(driver, LIST_URL)

    try:
        print("[INFO] 엑셀 다운로드 버튼 찾는 중...")
        wait = WebDriverWait(driver, 20)
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
        driver.set_script_timeout(10)
        driver.execute_script("fnPageExl('X14');")

    accept_alert_safe(driver, timeout=5)
    wait_for_download_complete(downloads_folder, timeout=1000)

finally:
    try:
        driver.quit()
    except Exception:
        pass

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
# 🚀 [변경] Supabase 전송 파이프라인
# =====================================================================
import requests
import json

print("🚀 Supabase로 데이터 전송 시작...")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE")

if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
    print("❌ Supabase 환경변수가 설정되지 않아 전송을 중지합니다.")
else:
    API_URL = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    
    auth_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    # 👇 [추가된 부분: 기존 데이터 싹 지우기] 👇
    print("🗑️ 기존 Supabase 데이터 삭제 중...")
    try:
        # Supabase API는 실수 방지를 위해 조건 없는 전체 삭제를 막아둡니다.
        # 따라서 'id 값이 비어있지 않은 모든 줄을 지워라' 라는 조건을 주어 전체 삭제를 유도합니다.
        delete_url = f"{API_URL}?id=not.is.null"
        requests.delete(delete_url, headers=auth_headers, timeout=60)
        print("✅ 기존 데이터 삭제 완료!")
    except Exception as e:
        print(f"❌ 데이터 삭제 통신 에러: {e}")
    # 👆 ------------------------------------- 👆

    # 데이터 입력용 헤더 설정
    insert_headers = auth_headers.copy()
    insert_headers["Content-Type"] = "application/json"
    insert_headers["Prefer"] = "return=minimal"

    # NaN 값은 에러를 유발하므로 빈 문자열로 처리
    records = df.fillna("").to_dict(orient="records")

    chunk_size = 3000
    total_chunks = (len(records) // chunk_size) + 1

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        try:
            response = requests.post(API_URL, headers=insert_headers, json=chunk, timeout=60)
            current_chunk = (i // chunk_size) + 1
            
            if response.status_code in [200, 201, 204]:
                print(f"📡 [{current_chunk}/{total_chunks}회차] 전송 성공")
            else:
                print(f"❌ [{current_chunk}/{total_chunks}회차] 실패: {response.text}")
                
        except Exception as e:
            print(f"❌ 전송 중 통신 에러 발생: {e}")

    print("✅ Supabase 싹 지우고 덮어쓰기 완료!")
