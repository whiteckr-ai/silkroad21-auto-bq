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


def wait_for_download_complete(dirpath: str, timeout: int = 180) -> None:
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
try:
    do_login(driver)
    goto_with_auth(driver, LIST_URL)

    # 사이트의 내보내기 JS 직접 호출 (X14: CSV)
    driver.execute_script("fnPageExl('X14');")
    accept_alert_safe(driver, timeout=2)

    # CSV 생성 대기
    wait_for_download_complete(downloads_folder, timeout=180)

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
