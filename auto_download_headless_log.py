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
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException
from google.cloud import bigquery

# ===== Stdout to log.txt (kept) =====
class DualLogger:
    def __init__(self, filepath):
        self.terminal = sys.__stdout__   # 원래 콘솔
        self.log = open(filepath, "w", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = sys.stderr = DualLogger("log.txt")

# ===== Environment / Settings (GitHub Actions & Local both) =====
RUNNER = os.getenv("GITHUB_ACTIONS") == "true"

# BigQuery
PROJECT_ID = os.getenv("GCP_PROJECT") or "savvy-mantis-457008-k6"
DATASET_ID = os.getenv("BQ_DATASET")  or "raw_data"
TABLE_ID   = os.getenv("BQ_TABLE")    or "goods_csv"

# Login (secrets override; local defaults stay as fallback)
LOGIN_ID = os.getenv("LOGIN_ID") or "ppazic"
LOGIN_PW = os.getenv("LOGIN_PW") or "123123"

# Download folder
if RUNNER:
    downloads_folder = str((Path.cwd() / "downloads").resolve())
else:
    downloads_folder = r"C:\\Users\\white\\Downloads\\csv"
Path(downloads_folder).mkdir(parents=True, exist_ok=True)

# GCP creds path (env first, else local file next to this script)
GOOGLE_CREDS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str((Path(__file__).parent / "bigquery-credentials.json").resolve()),
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDS

# URLs
LOGIN_URL = "https://silkroad21.co.kr/pzadm/Login.asp"
LIST_URL  = "https://silkroad21.co.kr/Admin/Acting/Acting_S.asp?gMnu1=101&gMnu2=10101"


# ===== Helper functions =====
def accept_alert_safe(driver, timeout: int = 3) -> bool:
    """알럿이 여러 번 뜨는 환경을 대비해서 연속 드레인."""
    appeared = False
    end = time.time() + timeout
    while time.time() < end:
        try:
            WebDriverWait(driver, 0.8).until(EC.alert_is_present())
            alert = driver.switch_to.alert
            print("[ALERT]", alert.text)
            alert.accept()
            appeared = True
            time.sleep(0.2)
        except Exception:
            time.sleep(0.2)
    return appeared


def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")        # ✅ 안정
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")   # ✅ runner 메모리 공유 이슈
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("prefs", {
        "download.default_directory": downloads_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "download.extensions_to_open": "",
    })
    # On GitHub runner, setup-chrome provides CHROME_PATH
    chrome_bin = os.getenv("CHROME_PATH")
    if chrome_bin:
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)

    # ✅ 타임아웃 상향 (기존 ReadTimeout 120s 문제 대응)
    driver.set_page_load_timeout(300)
    driver.set_script_timeout(300)
    driver.implicitly_wait(5)

    # ✅ headless 다운로드 확실히 허용 (CDP)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": downloads_folder}
        )
    except Exception as e:
        print("[WARN] setDownloadBehavior 실패:", e)

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

    # Handle possible alert & retry once
    if accept_alert_safe(driver, timeout=3):
        id_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "sMemId")))
        pw_el = driver.find_element(By.NAME, "sMemPw")
        id_el.clear(); id_el.send_keys(LOGIN_ID)
        pw_el.clear(); pw_el.send_keys(LOGIN_PW); pw_el.send_keys(Keys.RETURN)
        accept_alert_safe(driver, timeout=2)

    # Consider login done when URL changes away from Login.asp
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


def wait_for_download_complete(dirpath: str, timeout: int = 240) -> str:
    """
    - .crdownload 존재 → 다운로드 중
    - .csv(또는 기타) 처음 생성되는 순간부터 완료까지 대기
    """
    end = time.time() + timeout
    pattern_cr = os.path.join(dirpath, "*.crdownload")
    pattern_any = os.path.join(dirpath, "*")
    first_seen = None

    while time.time() < end:
        # 진행 중 파일(.crdownload) 있는지 체크
        if glob.glob(pattern_cr):
            time.sleep(0.8)
            continue

        files = [f for f in glob.glob(pattern_any) if not f.endswith(".crdownload")]
        csvs = [f for f in files if f.lower().endswith(".csv")]
        # 최초 생성 감지
        if csvs and first_seen is None:
            first_seen = max(csvs, key=os.path.getmtime)

        # 완료 판정: 진행 중 없고, csv 존재
        if csvs:
            latest = max(csvs, key=os.path.getmtime)
            # 파일 크기 안정화(마지막 1초간 크기 변화 없는지 확인)
            size1 = os.path.getsize(latest)
            time.sleep(1.0)
            size2 = os.path.getsize(latest)
            if size1 == size2:
                return latest

        time.sleep(0.8)

    raise TimeoutError("다운로드 완료 대기 시간 초과")


# === NEW: 내보내기 트리거를 안정적으로 수행 (클릭 우선 → JS 폴백, 재시도) ===
def trigger_export_stably(driver: webdriver.Chrome, wait: WebDriverWait, max_attempts: int = 3) -> None:
    """
    1) 화면에서 흔한 내보내기 버튼 후보를 순차적으로 탐색하여 클릭
    2) 실패 시 JS 함수(fnPageExl('X14')) 호출로 폴백
    각 단계에서 알럿 처리 및 재시도
    """
    selectors = [
        "#exportExcelBtn",
        "a#exportExcelBtn",
        "button#exportExcelBtn",
        "button.excel, a.excel, input.excel",
        "a[href*='Excel'], button[onclick*='Excel'], input[onclick*='Excel']",
        "a[onclick*='fnPageExl'], button[onclick*='fnPageExl'], input[onclick*='fnPageExl']",
    ]

    for attempt in range(1, max_attempts + 1):
        try:
            # 1) 클릭 방식 시도
            for sel in selectors:
                try:
                    el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                    print(f"[EXPORT] 클릭 시도(selector={sel})")
                    el.click()
                    time.sleep(0.5)
                    accept_alert_safe(driver, timeout=1)
                    return
                except Exception:
                    continue

            # 2) JS 직접 호출 폴백
            print("[EXPORT] JS 호출(fnPageExl('X14')) 시도")
            driver.execute_script("fnPageExl('X14');")
            time.sleep(0.5)
            accept_alert_safe(driver, timeout=1)
            return

        except Exception as e:
            print(f"[WARN] 내보내기 시도 {attempt}/{max_attempts} 실패:", repr(e))
            accept_alert_safe(driver, timeout=1)
            if attempt < max_attempts:
                time.sleep(2.5)

    raise RuntimeError("엑셀 내보내기 트리거 실패")


# ===== Main =====
driver = make_driver(headless=True)
try:
    do_login(driver)
    goto_with_auth(driver, LIST_URL)

    # (선택) 테이블/리스트 로딩 대기 — 페이지 구조에 맞게 커스터마이즈
    try:
        wait = WebDriverWait(driver, 30)
        # 흔한 리스트 영역 후보들 중 하나라도 로드되면 OK
        any_loaded = False
        for sel in ["#dataTable", ".list-table", "#divList", "table", ".grid"]:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                print(f"[READY] 리스트 로드 감지: {sel}")
                any_loaded = True
                break
            except Exception:
                continue
        if not any_loaded:
            print("[READY] 리스트 로드 신호를 못 찾았지만 계속 진행 (페이지 구조 미확인)")
    except Exception as e:
        print("[READY WARN] 리스트 대기 중 경고:", e)

    # === 변경 포인트: 내보내기 트리거를 안정적으로 ===
    trigger_export_stably(driver, WebDriverWait(driver, 20), max_attempts=3)

    # Wait for CSV
    latest_file = wait_for_download_complete(downloads_folder, timeout=240)
    print("⬇️ 다운로드 완료:", os.path.basename(latest_file))

finally:
    try:
        driver.quit()
    except Exception:
        pass

# Keep newest file only
csv_files = glob.glob(os.path.join(downloads_folder, "*.csv"))
if not csv_files:
    print("❌ CSV 파일이 존재하지 않습니다. (다운로드 실패)")
    sys.exit(1)
latest_file = max(csv_files, key=os.path.getctime)
for fp in csv_files:
    if fp != latest_file:
        os.remove(fp)
        print("🗑 삭제됨:", os.path.basename(fp))

# Load & clean
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
        base = c; i = 1
        while c in seen:
            c = f"{base}_{i}"; i += 1
        seen[c] = True; out.append(c)
    return out

df.columns = sanitize_columns(df.columns)
df = df.dropna(how="all").drop_duplicates()
print("🧹 데이터 정제 완료")

# Upload to BigQuery
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
