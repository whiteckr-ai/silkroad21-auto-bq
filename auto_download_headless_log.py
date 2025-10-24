from __future__ import annotations
import os, sys, time, glob, re
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
    def __init__(self, filepath):
        self.terminal = sys.__stdout__
        self.log = open(filepath, "w", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = sys.stderr = DualLogger("log.txt")

# ===== Environment =====
RUNNER = os.getenv("GITHUB_ACTIONS") == "true"
PROJECT_ID = os.getenv("GCP_PROJECT") or "savvy-mantis-457008-k6"
DATASET_ID = os.getenv("BQ_DATASET")  or "raw_data"
TABLE_ID   = os.getenv("BQ_TABLE")    or "goods_csv"
LOGIN_ID = os.getenv("LOGIN_ID") or "ppazic"
LOGIN_PW = os.getenv("LOGIN_PW") or "123123"

if RUNNER:
    downloads_folder = str((Path.cwd() / "downloads").resolve())
else:
    downloads_folder = r"C:\\Users\\white\\Downloads\\csv"
Path(downloads_folder).mkdir(parents=True, exist_ok=True)

GOOGLE_CREDS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str((Path(__file__).parent / "bigquery-credentials.json").resolve()),
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDS

LOGIN_URL = "https://silkroad21.co.kr/pzadm/Login.asp"
LIST_URL  = "https://silkroad21.co.kr/Admin/Acting/Acting_S.asp?gMnu1=101&gMnu2=10101"

# ===== Helper =====
def accept_alert_safe(driver, timeout=3):
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

def make_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
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

    chrome_bin = os.getenv("CHROME_PATH")
    if chrome_bin:
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(300)
    driver.set_script_timeout(300)
    driver.implicitly_wait(5)

    # ✅ Page + Browser 다운로드 허용
    for cmd, params in [
        ("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": downloads_folder}),
        ("Browser.setDownloadBehavior", {"behavior": "allowAndName", "downloadPath": downloads_folder})
    ]:
        try:
            driver.execute_cdp_cmd(cmd, params)
        except Exception as e:
            print(f"[WARN] {cmd} 실패:", e)

    return driver

def do_login(driver):
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)
    id_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemId")))
    pw_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemPw")))
    for el, val in ((id_el, LOGIN_ID), (pw_el, LOGIN_PW)):
        try: el.clear()
        except: pass
        el.send_keys(val)
    pw_el.send_keys(Keys.RETURN)
    accept_alert_safe(driver, 3)
    try:
        wait.until(lambda d: "Login.asp" not in d.current_url)
    except TimeoutException:
        raise RuntimeError("로그인 실패")

def goto_with_auth(driver, url, login_hint="Login.asp"):
    driver.get(url)
    if login_hint in driver.current_url:
        print("[INFO] 세션 만료, 재로그인")
        do_login(driver)
        driver.get(url)

def switch_to_new_window_if_any(driver, wait_sec=3):
    base = driver.current_window_handle
    start = time.time()
    while time.time() - start < wait_sec:
        handles = driver.window_handles
        if len(handles) > 1:
            for h in handles:
                if h != base:
                    driver.switch_to.window(h)
                    print("[WINDOW] 새 창 전환")
                    return True
        time.sleep(0.2)
    return False

def trigger_export_stably(driver, wait, max_attempts=3):
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
            for sel in selectors:
                try:
                    el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                    print(f"[EXPORT] 클릭 시도({sel})")
                    el.click()
                    time.sleep(0.5)
                    accept_alert_safe(driver, 1)
                    return
                except: continue
            print("[EXPORT] JS 호출(fnPageExl('X14')) 시도")
            driver.execute_script("fnPageExl('X14');")
            accept_alert_safe(driver, 1)
            return
        except Exception as e:
            print(f"[WARN] 내보내기 시도 {attempt} 실패:", e)
            time.sleep(2)
    raise RuntimeError("엑셀 내보내기 트리거 실패")

def wait_for_download_complete(dirpath, timeout=300, before_set=None):
    end = time.time() + timeout
    pattern_cr = os.path.join(dirpath, "*.crdownload")
    exts = (".csv", ".xls", ".xlsx", ".zip")
    def new_files():
        files = glob.glob(os.path.join(dirpath, "*"))
        if before_set: files = [f for f in files if f not in before_set]
        return files
    last_progress_ts = time.time()
    observed_anything = False
    while time.time() < end:
        if glob.glob(pattern_cr):
            observed_anything = True
            last_progress_ts = time.time()
            time.sleep(0.8)
            continue
        files = [f for f in new_files() if not f.endswith(".crdownload")]
        cand = [f for f in files if os.path.splitext(f)[1].lower() in exts]
        if cand:
            latest = max(cand, key=os.path.getmtime)
            sz1 = os.path.getsize(latest)
            time.sleep(1)
            sz2 = os.path.getsize(latest)
            if sz1 == sz2:
                return latest
            else:
                last_progress_ts = time.time()
        if time.time() - last_progress_ts > 5 and not observed_anything:
            raise TimeoutError("NO_PROGRESS")
        time.sleep(0.8)
    raise TimeoutError("다운로드 완료 대기 시간 초과")

# ===== Main =====
driver = make_driver(headless=True)
try:
    do_login(driver)
    goto_with_auth(driver, LIST_URL)
    wait = WebDriverWait(driver, 30)
    for sel in ["#dataTable", ".list-table", "#divList", "table", ".grid"]:
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            print(f"[READY] 리스트 로드 감지: {sel}")
            break
        except: continue
    before_set = set(glob.glob(os.path.join(downloads_folder, "*")))
    trigger_export_stably(driver, WebDriverWait(driver, 20))
    switch_to_new_window_if_any(driver, 3)
    try:
        latest_file = wait_for_download_complete(downloads_folder, 300, before_set)
    except TimeoutError as te:
        if str(te) == "NO_PROGRESS":
            print("[RETRY] 다운로드 진척 없음 → JS 폴백 재트리거")
            driver.execute_script("fnPageExl('X14');")
            accept_alert_safe(driver, 1)
            switch_to_new_window_if_any(driver, 3)
            latest_file = wait_for_download_complete(downloads_folder, 300, before_set)
        else:
            raise
    print("⬇️ 다운로드 완료:", os.path.basename(latest_file))
finally:
    try: driver.quit()
    except: pass

# ===== File clean & upload =====
csv_files = glob.glob(os.path.join(downloads_folder, "*.csv")) + \
             glob.glob(os.path.join(downloads_folder, "*.xls")) + \
             glob.glob(os.path.join(downloads_folder, "*.xlsx"))
if not csv_files:
    print("❌ 파일이 존재하지 않습니다.")
    sys.exit(1)
latest_file = max(csv_files, key=os.path.getctime)
for fp in csv_files:
    if fp != latest_file:
        os.remove(fp)
        print("🗑 삭제됨:", os.path.basename(fp))

try:
    df = pd.read_csv(latest_file, encoding="utf-8-sig", dtype=str, on_bad_lines="skip")
except Exception:
    df = pd.read_csv(latest_file, encoding="cp949", dtype=str, on_bad_lines="skip")
print(f"📊 데이터 로딩 완료: {len(df)} rows")

def sanitize_columns(cols):
    seen, out = {}, []
    for c in cols:
        c = (c or "").strip()
        c = re.sub(r"[^\w]", "_", c)
        if re.match(r"^\d", c): c = "_" + c
        base = c; i = 1
        while c in seen:
            c = f"{base}_{i}"; i += 1
        seen[c] = True; out.append(c)
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
