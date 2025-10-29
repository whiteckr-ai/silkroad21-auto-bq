from __future__ import annotations
import os, sys, time, glob, re, json, base64
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
    from selenium.webdriver.remote.remote_connection import RemoteConnection
    RemoteConnection.set_timeout(300)  # 기본 120s → 여유
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

    # 성능 로그(네트워크 이벤트) 활성화 (CDP 폴백용)
    perf_prefs = {"enableNetwork": True, "enablePage": False}
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.set_capability("goog:perfLoggingPrefs", perf_prefs)

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(300)
    driver.set_script_timeout(300)
    driver.implicitly_wait(5)

    # Page/Browser 다운로드 허용 (가능 환경에서 동작)
    for cmd, params in [
        ("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": downloads_folder}),
        ("Browser.setDownloadBehavior", {"behavior": "allowAndName", "downloadPath": downloads_folder})
    ]:
        try:
            driver.execute_cdp_cmd(cmd, params)
        except Exception as e:
            print(f"[WARN] {cmd} 실패:", e)

    # CDP Network enable
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception as e:
        print("[WARN] Network.enable 실패:", e)

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

# --- 다중 버튼 대비: 후보를 모아 우선순위대로 클릭 ---
def trigger_export_stably(driver, wait, max_attempts=2):
    # 1) 명시적 id 우선
    priority_selectors = [
        "#exportExcelBtn",
        "a#exportExcelBtn",
        "button#exportExcelBtn",
    ]
    # 2) 흔한 클래스/속성
    generic_selectors = [
        "button.excel, a.excel, input.excel",
        "a[href*='Excel'], button[onclick*='Excel'], input[onclick*='Excel']",
        "a[onclick*='fnPageExl'], button[onclick*='fnPageExl'], input[onclick*='fnPageExl']",
        "a[download], button[download]",
    ]
    # 3) 텍스트 기반 (엑셀/Excel/다운로드)
    text_xpaths = [
        "//a[normalize-space()[contains(., '엑셀') or contains(., 'Excel') or contains(., '다운로드')]]",
        "//button[normalize-space()[contains(., '엑셀') or contains(., 'Excel') or contains(., '다운로드')]]",
        "//input[@type='button' or @type='submit'][contains(@value,'엑셀') or contains(@value,'Excel') or contains(@value,'다운로드')]",
    ]

    def visible_and_enabled(el):
        try:
            return el.is_displayed() and el.is_enabled()
        except Exception:
            return False

    def collect_candidates():
        seen, candidates = set(), []
        for sel in priority_selectors + generic_selectors:
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.id in seen: continue
                    if visible_and_enabled(el):
                        candidates.append(("css", sel, el))
                        seen.add(el.id)
            except Exception:
                pass
        for xp in text_xpaths:
            try:
                for el in driver.find_elements(By.XPATH, xp):
                    if el.id in seen: continue
                    if visible_and_enabled(el):
                        candidates.append(("xpath", xp, el))
                        seen.add(el.id)
            except Exception:
                pass
        return candidates

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            # 우선 명시적 → 텍스트/onclick 순
            cands = collect_candidates()
            # 우선순위 재정렬: id우선, 그다음 onclick*fnPageExl, 그다음 텍스트
            def score(item):
                kind, spec, el = item
                onclick = (el.get_attribute("onclick") or "").lower()
                id_attr = (el.get_attribute("id") or "").lower()
                text = (el.text or "").strip()
                s = 0
                if id_attr == "exportexcelbtn": s += 100
                if "fnpageexl" in onclick: s += 50
                if any(k in text for k in ("엑셀", "Excel", "다운로드")): s += 10
                return -s
            cands.sort(key=score)

            for kind, spec, el in cands:
                try:
                    desc = f"{kind}:{spec}"
                    print(f"[EXPORT] 클릭 시도({desc})")
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.1)
                    el.click()
                    time.sleep(0.5)
                    accept_alert_safe(driver, 1)
                    return
                except Exception as e:
                    last_error = e
                    continue

            # 후보가 하나도 없으면 JS 직접 호출
            print("[EXPORT] JS 호출(fnPageExl('X14')) 시도")
            driver.execute_script("fnPageExl('X14');")
            accept_alert_safe(driver, 1)
            return
        except Exception as e:
            last_error = e
            print(f"[WARN] 내보내기 시도 {attempt} 실패:", e)
            time.sleep(2)
    raise RuntimeError(f"엑셀 내보내기 트리거 실패: {last_error}")

# --- 상태 기반 다운로드 감시 (mtime 의존 안 함) ---
def wait_for_download_complete(folder: str, timeout: int = 300):
    start = time.time()
    baseline = set(os.listdir(folder))
    last_sizes = {}
    def list_cr():
        return [f for f in os.listdir(folder) if f.endswith(".crdownload")]
    def fsize(p):
        try: return os.path.getsize(p)
        except FileNotFoundError: return -1

    while True:
        # 새로 생긴 완성 파일 (ctime 기준) 우선 확인
        curr = set(os.listdir(folder))
        new_entries = curr - baseline
        completed = [f for f in new_entries if not f.endswith(".crdownload")]
        if completed:
            candidates = sorted(
                (os.path.join(folder, f) for f in completed),
                key=lambda p: os.path.getctime(p),
                reverse=True,
            )
            return candidates[0]

        # 진행 중(.crdownload) 감시
        crs = list_cr()
        if crs:
            progressed = False
            for f in crs:
                p = os.path.join(folder, f)
                sz = fsize(p)
                if p not in last_sizes or sz > last_sizes[p]:
                    progressed = True
                last_sizes[p] = sz
            if progressed:
                pass  # 계속 대기
            else:
                if time.time() - start > timeout:
                    raise TimeoutError("DOWNLOAD_STALLED")
        else:
            # .crdownload 없음 → 시작 안 됐거나 이미 끝난 상태
            complete_files = [f for f in os.listdir(folder) if not f.endswith(".crdownload")]
            if complete_files:
                recent = [os.path.join(folder, f) for f in complete_files
                          if os.path.getctime(os.path.join(folder, f)) >= start]
                if recent:
                    return sorted(recent, key=os.path.getctime, reverse=True)[0]

        if time.time() - start > timeout:
            raise TimeoutError("NO_PROGRESS")
        time.sleep(1.0)

# --- ★ CDP 성능 로그로 '첨부파일 응답'을 잡아 저장 ---
def cdp_capture_attachment_to_file(driver, out_dir, timeout=120):
    """
    performance log 의 Network.responseReceived 에서
    Content-Disposition: attachment 응답을 찾아
    Network.getResponseBody 로 바디를 저장.
    """
    start = time.time()
    os.makedirs(out_dir, exist_ok=True)

    def iter_perf_logs():
        try:
            logs = driver.get_log("performance")
        except Exception:
            return []
        events = []
        for entry in logs:
            try:
                msg = json.loads(entry.get("message", "{}")).get("message", {})
                events.append(msg)
            except Exception:
                continue
        return events

    target_req_id = None
    filename = None
    mime_hint = None

    while time.time() - start < timeout:
        for ev in iter_perf_logs():
            if ev.get("method") == "Network.responseReceived":
                params = ev.get("params", {})
                res = params.get("response", {})
                headers = {k.lower(): v for k, v in (res.get("headers", {}) or {}).items()}
                cd = headers.get("content-disposition", "")
                if "attachment" in cd.lower():
                    fname = "download.bin"
                    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)"?', cd, flags=re.I)
                    if m: fname = m.group(1)
                    ctype = headers.get("content-type", "")
                    mime_hint = ctype
                    target_req_id = params.get("requestId")
                    filename = fname
                    print(f"[CDP] attachment 응답 감지: requestId={target_req_id}, filename={filename}, content-type={ctype}")
                    break
        if target_req_id:
            break
        time.sleep(0.3)

    if not target_req_id:
        raise TimeoutError("CDP: 첨부파일 응답을 찾지 못했습니다.")

    body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": target_req_id})
    data = body.get("body", "")
    encoded = body.get("base64Encoded", False)
    content = base64.b64decode(data) if encoded else data.encode("utf-8", errors="ignore")

    out_name = filename
    if "." not in out_name and mime_hint:
        if "csv" in mime_hint: out_name += ".csv"
        elif "excel" in mime_hint or "spreadsheet" in mime_hint: out_name += ".xlsx"
        elif "zip" in mime_hint: out_name += ".zip"

    out_path = os.path.join(out_dir, out_name)
    with open(out_path, "wb") as f:
        f.write(content)
    print(f"[CDP] 파일 저장 완료: {out_path}")
    return out_path

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

    # 1) 트리거(여러 버튼/텍스트 후보 → 우선순위 클릭, 실패 시 JS 폴백)
    trigger_export_stably(driver, WebDriverWait(driver, 20))
    switch_to_new_window_if_any(driver, 3)

    # 2) 상태 기반 다운로드 감시
    latest_file = None
    try:
        latest_file = wait_for_download_complete(downloads_folder, 300)
        print("⬇️ 다운로드 완료:", os.path.basename(latest_file))
    except TimeoutError as te:
        print("[INFO] 일반 다운로드 감시 실패:", te)
        # 폴더에 혹시 이미 완성 파일이 있으면 그걸 채택
        cand = []
        for ext in ("*.csv", "*.xls", "*.xlsx", "*.zip"):
            cand += glob.glob(os.path.join(downloads_folder, ext))
        if cand:
            latest_file = max(cand, key=os.path.getctime)
            print("⬇️ 폴더 재검사로 파일 채택:", os.path.basename(latest_file))
        else:
            # 최종 폴백: CDP로 직접 저장
            latest_file = cdp_capture_attachment_to_file(driver, downloads_folder, timeout=180)

finally:
    try: driver.quit()
    except: pass

# ===== File clean & upload =====
# csv/xls/xlsx/zip 중 최신 파일 선택
cand_files = []
for ext in ("*.csv", "*.xls", "*.xlsx", "*.zip"):
    cand_files += glob.glob(os.path.join(downloads_folder, ext))
if not cand_files:
    print("❌ 파일이 존재하지 않습니다.")
    sys.exit(1)

latest_file = max(cand_files, key=os.path.getctime)
for fp in list(cand_files):
    if fp != latest_file:
        try:
            os.remove(fp)
            print("🗑 삭제됨:", os.path.basename(fp))
        except Exception:
            pass

# ZIP 처리 등은 필요 시 확장 가능. 우선 CSV/엑셀 로딩
df = None
load_err = None
if latest_file.lower().endswith(".csv"):
    try:
        df = pd.read_csv(latest_file, encoding="utf-8-sig", dtype=str, on_bad_lines="skip")
    except Exception as e1:
        try:
            df = pd.read_csv(latest_file, encoding="cp949", dtype=str, on_bad_lines="skip")
        except Exception as e2:
            load_err = (e1, e2)
elif latest_file.lower().endswith((".xls", ".xlsx")):
    try:
        df = pd.read_excel(latest_file, dtype=str)
    except Exception as e:
        load_err = e
else:
    load_err = f"지원하지 않는 확장자: {latest_file}"

if df is None:
    print("❌ 데이터 로딩 실패:", load_err)
    sys.exit(1)

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
