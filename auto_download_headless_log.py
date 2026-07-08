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
from selenium.common.exceptions import TimeoutException, WebDriverException
from google.cloud import bigquery
import google.auth
import urllib3.exceptions
import requests
import json
import gspread
from gspread_dataframe import set_with_dataframe

RETRYABLE_ERRORS = (
    TimeoutException,
    WebDriverException,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.ProtocolError,
    TimeoutError,
    ConnectionError,
)

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
PROJECT_ID = os.environ["GCP_PROJECT"]
DATASET_ID = os.getenv("BQ_DATASET") or "raw_data"
TABLE_ID = os.getenv("BQ_TABLE") or "goods_csv"

# Login
LOGIN_ID = os.environ["LOGIN_ID"]
LOGIN_PW = os.environ["LOGIN_PW"]

# Google Sheets
GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET") or "raw_data"

# Customer tabs config (회원고유번호 → 탭 이름 매핑)
# JSON 형식: {"회원고유번호1": "탭이름1", ...}
CUSTOMER_TABS_JSON = os.getenv("GSHEET_CUSTOMER_TABS", "{}")
try:
    CUSTOMER_TABS = json.loads(CUSTOMER_TABS_JSON)
except json.JSONDecodeError as e:
    print(f"⚠️ GSHEET_CUSTOMER_TABS JSON 파싱 실패: {e}")
    CUSTOMER_TABS = {}

CUSTOMER_ID_COLUMN = "회원고유번호"

# ===== 파생 컬럼 규칙 (담당팀 매핑) =====
# WPS 수식:
# IF(OR([담당자1]="최국화",[담당자1]="김춘매",[담당자1]="장옥선",[담당자1]="서연연"), "C-TEAM",
#  IF(OR([담당자1]="박명숙",[담당자1]="지연니"), "A-TEAM",
#   IF(OR([담당자1]="장춘봉",[담당자1]="왕챈",[담당자1]="진진"), "B-TEAM",
#    IF(OR([담당자1]="양호원"), "박기훈팀", "팀배정필요"))))
담당팀_매핑 = {
    "최국화": "C-TEAM",
    "김춘매": "C-TEAM",
    "장옥선": "C-TEAM",
    "서연연": "C-TEAM",
    "박명숙": "A-TEAM",
    "지연니": "A-TEAM",
    "장춘봉": "B-TEAM",
    "왕챈":   "B-TEAM",
    "진진":   "B-TEAM",
    "양호원": "박기훈팀",
}
담당팀_기본값 = "팀배정필요"

def apply_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """BigQuery 업로드 전, 계산/매핑이 필요한 파생 컬럼을 추가합니다.
    새 파생 컬럼이 필요해지면 이 함수 안에만 추가하면 됩니다."""

    # 1) 담당팀 (담당자1 → 조건부 매핑)
    if "담당자1" in df.columns:
        df["담당팀"] = df["담당자1"].map(담당팀_매핑).fillna(담당팀_기본값)

        누락 = df.loc[df["담당팀"] == 담당팀_기본값, "담당자1"].unique()
        누락 = [v for v in 누락 if v not in (None, "", "nan")]
        if len(누락) > 0:
            print(f"⚠️ 담당팀 매핑 안 된 '담당자1' 값: {list(누락)}")
    else:
        print("⚠️ '담당자1' 컬럼 없음 → '담당팀' 생성 건너뜀")

    # 2) 합계 (수량 * 단가)
    if "수량" in df.columns and "단가" in df.columns:
        df["합계"] = (
            pd.to_numeric(df["수량"], errors="coerce")
            * pd.to_numeric(df["단가"], errors="coerce")
        )
    else:
        print("⚠️ '수량' 또는 '단가' 컬럼 없음 → '합계' 생성 건너뜀")

    return df

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
        driver.command_executor._client_config.timeout = 300
    except AttributeError:
        try:
            driver.command_executor.set_timeout(300)
        except Exception:
            pass

    driver.set_script_timeout(60)
    driver.set_page_load_timeout(180)

    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": downloads_folder},
        )
    except Exception:
        pass

    driver.implicitly_wait(5)
    return driver

def do_login(driver: webdriver.Chrome, max_retries: int = 3) -> None:
    wait = WebDriverWait(driver, 20)
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] 로그인 시도 {attempt}/{max_retries}")
            driver.get(LOGIN_URL)

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
                btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
                btn.click()
                WebDriverWait(driver, 10).until(lambda d: "Login.asp" not in d.current_url)

            print("[INFO] 로그인 성공")
            return

        except RETRYABLE_ERRORS as e:
            last_error = e
            print(f"[WARN] 로그인 시도 {attempt} 실패: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                wait_sec = 15 * attempt
                print(f"[INFO] {wait_sec}초 후 재시도합니다...")
                time.sleep(wait_sec)

    raise RuntimeError(f"로그인 {max_retries}회 모두 실패. 마지막 에러: {last_error}")

def goto_with_auth(driver: webdriver.Chrome, url: str, login_hint: str = "Login.asp", max_retries: int = 3) -> None:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] 페이지 이동 시도 {attempt}/{max_retries}: {url}")
            driver.get(url)
            time.sleep(0.5)
            if login_hint in driver.current_url:
                print("[INFO] 로그인 페이지로 리다이렉트됨, 재로그인 진행")
                do_login(driver)
                driver.get(url)
            return

        except RETRYABLE_ERRORS as e:
            last_error = e
            print(f"[WARN] 페이지 이동 시도 {attempt} 실패: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                wait_sec = 15 * attempt
                print(f"[INFO] {wait_sec}초 후 재시도합니다...")
                time.sleep(wait_sec)

    raise RuntimeError(f"페이지 이동 {max_retries}회 모두 실패. 마지막 에러: {last_error}")

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

def push_df_to_worksheet(spreadsheet, tab_name: str, df_data: pd.DataFrame) -> None:
    """주어진 탭에 데이터프레임을 클리어 후 쓰기. 탭 없으면 생성."""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        print(f"[INFO] 탭 '{tab_name}' 없음 → 새로 생성")
        ws = spreadsheet.add_worksheet(
            title=tab_name,
            rows=max(len(df_data) + 100, 100),
            cols=max(len(df_data.columns) + 5, 26),
        )

    ws.clear()

    if len(df_data) > 0:
        set_with_dataframe(
            ws,
            df_data,
            include_index=False,
            include_column_header=True,
            resize=True,
        )
    else:
        # 빈 결과면 헤더만 쓰기
        set_with_dataframe(
            ws,
            df_data.iloc[0:0],
            include_index=False,
            include_column_header=True,
            resize=False,
        )


# ===== Main =====
driver = make_driver(headless=True)
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
    wait_for_download_complete(downloads_folder, timeout=120)

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

print(f"📊 데이터 로딩 완료: {len(df)} rows × {len(df.columns)} cols")

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

# BQ 적재용 (컬럼명 sanitize 필요)
df_bq = df.copy()
df_bq.columns = sanitize_columns(df_bq.columns)
df_bq = df_bq.dropna(how="all").drop_duplicates()
print("🧹 BQ용 데이터 정제 완료")

# ⭐ 파생 컬럼 추가 (담당팀, 합계) — 여기서 처리하면 BigQuery/OneDrive/KDocs 모두 자동 반영
df_bq = apply_derived_columns(df_bq)
print(f"➕ 파생 컬럼 추가 완료. 현재 컬럼: {list(df_bq.columns)}")

client = bigquery.Client(project=PROJECT_ID)
full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
job = client.load_table_from_dataframe(
    df_bq,
    full_table_id,
    location="asia-northeast3",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
)
job.result()
print(f"✅ BigQuery 업로드 성공: {len(df_bq)}건 → {full_table_id}")

# =====================================================================
# 📊 Google Sheets 푸시 (raw_data 전체 탭 + 고객사별 분할 탭)
# =====================================================================
print("📊 Google Sheets로 데이터 전송 시작...")

if not GSHEET_ID:
    print("⚠️ GSHEET_ID 환경변수가 설정되지 않아 Sheets 전송을 건너뜁니다.")
else:
    try:
        creds, _ = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(GSHEET_ID)
        print(f"[INFO] 스프레드시트 열기 성공: {spreadsheet.title}")

        # --- 1) 전체 raw_data 탭 (관리자/백업용) ---
        try:
            print(f"[INFO] raw_data 탭 푸시 시작 ({len(df):,}건)")
            push_df_to_worksheet(spreadsheet, GSHEET_WORKSHEET, df)
            print(f"✅ raw_data 푸시 완료: {len(df):,}건 → {GSHEET_WORKSHEET}")
        except Exception as e:
            print(f"❌ raw_data 푸시 실패: {type(e).__name__}: {e}")

        # --- 2) 고객사별 분할 탭 ---
        if not CUSTOMER_TABS:
            print("[INFO] CUSTOMER_TABS 비어있음. 고객사 분할 탭 건너뜀.")
        elif CUSTOMER_ID_COLUMN not in df.columns:
            print(f"⚠️ '{CUSTOMER_ID_COLUMN}' 컬럼이 데이터에 없음. 고객사 분할 탭 건너뜀.")
            print(f"[DIAG] 사용 가능한 컬럼: {list(df.columns)[:10]}...")
        else:
            # 회원고유번호 컬럼을 문자열로 정규화 (비교 시 일치하도록)
            df_normalized = df.copy()
            df_normalized[CUSTOMER_ID_COLUMN] = (
                df_normalized[CUSTOMER_ID_COLUMN].astype(str).str.strip()
            )

            print(f"📊 고객사 분할 탭 생성 시작 ({len(CUSTOMER_TABS)}개)")

            for member_id, tab_name in CUSTOMER_TABS.items():
                try:
                    member_id_str = str(member_id).strip()
                    df_customer = df_normalized[
                        df_normalized[CUSTOMER_ID_COLUMN] == member_id_str
                    ]

                    print(f"  [{member_id} → {tab_name}] 매칭: {len(df_customer):,}건")
                    push_df_to_worksheet(spreadsheet, tab_name, df_customer)
                    print(f"  ✅ {tab_name} 완료")

                except Exception as e:
                    print(f"  ❌ {member_id} ({tab_name}) 실패: {type(e).__name__}: {e}")
                    # 한 고객사 실패해도 다른 고객사는 계속 진행
                    continue

            print(f"✅ 고객사 분할 탭 처리 완료")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ Spreadsheet를 찾을 수 없음. GSHEET_ID 또는 공유 권한 확인 필요.")
    except gspread.exceptions.APIError as e:
        print(f"❌ Google Sheets API 에러: {e}")
    except Exception as e:
        import traceback
        print(f"❌ Google Sheets 전송 실패: {type(e).__name__}: {e}")
        traceback.print_exc()

# =====================================================================
# 🚀 Supabase 전송 파이프라인
# =====================================================================
print("🚀 Supabase로 데이터 전송 시작...")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE")

if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
    print("❌ Supabase 환경변수가 설정되지 않아 전송을 중지합니다.")
else:
    df_sup = df_bq.copy()

    if '아이템번호' in df_sup.columns:
        df_sup['아이템번호'] = df_sup['아이템번호'].astype(str).str.strip()
        df_sup = df_sup.drop_duplicates(subset=['아이템번호'], keep='last')
        print(f"🧹 Supabase용 중복 제거 완료. 남은 데이터: {len(df_sup)}건")

    df_sup = df_sup.astype(object).where(pd.notnull(df_sup), None)
    records = df_sup.to_dict(orient="records")

    for row in records:
        for key, value in row.items():
            if isinstance(value, str):
                cleaned_val = value.strip()
                if cleaned_val in ["", "nan", "None", "<NA>", "NaT"]:
                    row[key] = None
                else:
                    row[key] = cleaned_val

    API_URL = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    auth_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    insert_headers = auth_headers.copy()
    insert_headers["Content-Type"] = "application/json"
    insert_headers["Prefer"] = "return=minimal, resolution=merge-duplicates"

    upsert_url = f"{API_URL}?on_conflict=아이템번호"

import math

chunk_size = 500          # 3000은 timeout 유발 → 작게. 그래도 죽으면 200~300까지 낮춰봐
total_chunks = math.ceil(len(records) / chunk_size)   # +1 방식은 딱 떨어질 때 1 과다
failed_chunks = []

for idx, i in enumerate(range(0, len(records), chunk_size), start=1):
    chunk = records[i : i + chunk_size]
    success, last_err = False, None

    for attempt in range(1, 6):                # 청크당 최대 5회 진짜 재시도
        try:
            response = requests.post(upsert_url, headers=insert_headers,
                                     json=chunk, timeout=120)
            if response.status_code in (200, 201, 204):
                print(f"📡 [{idx}/{total_chunks}] upsert 성공 ({len(chunk)}건)")
                success = True
                break
            last_err = f"HTTP {response.status_code}: {response.text[:200]}"
            print(f"⚠️ [{idx}/{total_chunks}] 시도 {attempt} 실패: {last_err}")
        except Exception as e:
            last_err = str(e)
            print(f"⚠️ [{idx}/{total_chunks}] 시도 {attempt} 통신에러: {last_err}")
        time.sleep(2 * attempt)               # 지수 백오프

    if not success:
        failed_chunks.append((idx, last_err))
        print(f"❌ [{idx}/{total_chunks}] 최종 실패")

if failed_chunks:
    print(f"🚨 Supabase 실패: {len(failed_chunks)}/{total_chunks} 청크 / "
          f"약 {len(failed_chunks) * chunk_size:,}건 유실 가능")
    sys.exit(1)                               # 파이프라인을 '실패'로 종료해야 알아챔
else:
    print(f"✅ Supabase 전송 완료: 전체 {len(records):,}건")
    print("🎉 크롤링 -> BigQuery -> Sheets -> Supabase 파이프라인 완료!")
    print("🎉 크롤링 -> BigQuery -> Sheets(전체+고객사) -> Supabase 모든 자동화 파이프라인 완료!")
