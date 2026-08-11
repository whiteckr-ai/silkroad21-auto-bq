from __future__ import annotations

# ===== Imports =====
import os
import sys
import time
import glob
import re
import unicodedata
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

    # 3) 대행구분 (환율 == 0 → 배송대행, 그 외(0이 아니거나 값 없음) → 구매대행)
    if "환율" in df.columns:
        환율_숫자 = pd.to_numeric(df["환율"], errors="coerce")
        df["대행구분"] = 환율_숫자.apply(lambda v: "배송대행" if v == 0 else "구매대행")
    else:
        print("⚠️ '환율' 컬럼 없음 → '대행구분' 생성 건너뜀")

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
    """주어진 탭에 데이터프레임을 씀. 탭 없으면 생성.
    Clear 없이 덮어쓰기 → 남는 행/열만 나중에 정리 (XLOOKUP 등 참조 중 빈 시트 노출 방지)"""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        print(f"[INFO] 탭 '{tab_name}' 없음 → 새로 생성")
        ws = spreadsheet.add_worksheet(
            title=tab_name,
            rows=max(len(df_data) + 100, 100),
            cols=max(len(df_data.columns) + 5, 26),
        )

    # 기존에 데이터가 차지하던 행/열 크기 (지우기 전에 미리 확인)
    old_row_count = ws.row_count
    old_col_count = ws.col_count

    new_row_count = len(df_data) + 1  # 헤더 포함
    new_col_count = len(df_data.columns) if len(df_data.columns) > 0 else 1

    if len(df_data) > 0:
        set_with_dataframe(
            ws,
            df_data,
            include_index=False,
            include_column_header=True,
            resize=False,
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

    # 새 데이터가 예전보다 행/열이 적을 때만, 남는 구간을 마지막에 정리
    if old_row_count > new_row_count or old_col_count > new_col_count:
        try:
            ws.resize(rows=new_row_count, cols=new_col_count)
        except Exception as e:
            print(f"[WARN] 남는 구간 정리 실패 (resize): {e}")


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

print("🎉 크롤링 -> BigQuery -> Sheets(전체+고객사) 자동화 파이프라인 완료!")


# =====================================================================
# 📦 [추가] 패킹 서버로 item_master 전송 (기존 로직 뒤, 실패해도 영향 없음)
# =====================================================================
try:
    _packing_url = os.getenv("PACKING_INGEST_URL")
    _packing_key = os.getenv("PACKING_INGEST_KEY", "")
    if _packing_url:
        print("📦 패킹 서버로 item_master 전송 시작...")
        _col = {
            "item_no": "아이템번호", "member_name": "회원명", "member_id": "회원고유번호",
            "product": "상품명", "price": "단가", "url": "상품URL",
            "thumbnail_url": "이미지URL",
            "inspection_url": "실사주소",
            "inspect_opt": "구매대행_신청_옵션", "partial_qty": "부분정밀검수_수량",
            "team": "담당팀", "agency": "대행구분",
            "total_qty": "수량",
            "order_status": "주문상태",          
            "buy_rate": "환율",
            "color": "색상",
            "name_en": "통관품목",
            "arrival_date": "도착일",
            "inspect_date": "검품완료일",
        }
        def _header_key(value):
            """CSV 헤더의 BOM·개행·공백·구분자 차이를 제거해 같은 컬럼을 찾는다."""
            text = unicodedata.normalize("NFKC", str(value or ""))
            text = text.replace("\ufeff", "").replace("\u200b", "")
            return re.sub(r"[\s_\-]+", "", text).lower()

        def _resolve_header(*candidates):
            by_key = {_header_key(col): col for col in df_bq.columns}
            for candidate in candidates:
                actual = by_key.get(_header_key(candidate))
                if actual is not None:
                    return actual
            return ""

        # 다운로드 사이트의 헤더에 숨은 개행/공백이 붙어도 실사주소가 누락되지 않게 한다.
        _col["thumbnail_url"] = _resolve_header("이미지URL", "이미지주소", "썸네일URL")
        _col["inspection_url"] = _resolve_header("실사주소", "실사URL", "실사이미지URL")
        import math as _math
        def _num(v):
            try:
                f = float(v)
                if _math.isnan(f) or _math.isinf(f):
                    return 0
                return f
            except (TypeError, ValueError):
                return 0
        def _text(v):
            """pandas 결측값이 문자열 'nan'/'None'으로 전송되지 않게 정규화."""
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            return "" if s.lower() in ("nan", "none", "nat") else s

        def _url_text(v):
            """일반 URL뿐 아니라 HTML 링크/엑셀 HYPERLINK 형태도 실제 주소로 정규화한다."""
            s = _text(v)
            if not s:
                return ""
            html_match = re.search(r"""href\s*=\s*["']([^"']+)["']""", s, re.I)
            if html_match:
                return html_match.group(1).strip()
            formula_match = re.search(r"""HYPERLINK\s*\(\s*["']([^"']+)["']""", s, re.I)
            if formula_match:
                return formula_match.group(1).strip()
            return s

        if not _col["thumbnail_url"]:
            print(
                "⚠️ 썸네일 컬럼 '이미지URL' 없음 "
                "→ thumbnail_url은 빈 값으로 전송합니다."
            )
        else:
            print(f"[INFO] 썸네일 원본 컬럼 확인: {_col['thumbnail_url']!r}")
        if not _col["inspection_url"]:
            similar = [col for col in df_bq.columns if "실사" in str(col)]
            print(
                "⚠️ 실사 컬럼 '실사주소' 없음 "
                f"(실사 포함 헤더: {similar}) → inspection_url은 빈 값으로 전송합니다."
            )
        else:
            print(f"[INFO] 실사 원본 컬럼 확인: {_col['inspection_url']!r}")

        _records = []
        for _, _r in df_bq.iterrows():
            _rd = _r.to_dict()
            _ino = _text(_rd.get(_col["item_no"], ""))
            if not _ino:
                continue
            _records.append({
                "item_no": _ino,
                "member_name": _text(_rd.get(_col["member_name"], "")),
                "member_id": _text(_rd.get(_col["member_id"], "")),
                "product": _text(_rd.get(_col["product"], "")),
                "price": _num(_rd.get(_col["price"])),
                "url": _text(_rd.get(_col["url"], "")),
                "thumbnail_url": _url_text(_rd.get(_col["thumbnail_url"], "")),
                "inspection_url": _url_text(_rd.get(_col["inspection_url"], "")),
                "inspect_opt": _text(_rd.get(_col["inspect_opt"], "")).replace("\t", " "),
                "partial_qty": _num(_rd.get(_col["partial_qty"])),
                "team": _text(_rd.get(_col["team"], "")),
                "agency": _text(_rd.get(_col["agency"], "")),
                "total_qty": _num(_rd.get(_col["total_qty"])),
                "order_status": _text(_rd.get(_col["order_status"], "")),
                "buy_rate": _num(_rd.get(_col["buy_rate"])),
                "color": _text(_rd.get(_col["color"], "")),
                "name_en": _text(_rd.get(_col["name_en"], "")),
                "arrival_date": _text(_rd.get(_col["arrival_date"], "")),
                "inspect_date": _text(_rd.get(_col["inspect_date"], "")),
            })
        _thumb_count = sum(1 for _rec in _records if _rec.get("thumbnail_url"))
        _inspection_count = sum(1 for _rec in _records if _rec.get("inspection_url"))
        print(f"[INFO] 썸네일 URL 포함: {_thumb_count:,}/{len(_records):,}건")
        print(f"[INFO] 실사주소 포함: {_inspection_count:,}/{len(_records):,}건")
        _resp = requests.post(f"{_packing_url}?k={_packing_key}", json={"items": _records}, timeout=120)
        if _resp.status_code == 200:
            print(f"✅ 패킹 서버 전송 완료: {_resp.json()}")
        else:
            print(f"❌ 패킹 서버 전송 실패: {_resp.status_code} {_resp.text[:200]}")
    else:
        print("[INFO] PACKING_INGEST_URL 미설정 → 패킹 전송 건너뜀")
except Exception as _e:
    print(f"❌ 패킹 서버 전송 중 오류(무시): {type(_e).__name__}: {_e}")


# =====================================================================
# 📦 [추가] 재무회계 ERP로 담당자 작업기록 전송 (근태 교차확인용)
# =====================================================================
try:
    _fin_url = os.getenv("FINANCE_INGEST_URL")            # 예: http://<서버IP>:8080/api/packing/ingest
    _fin_key = os.getenv("FINANCE_INGEST_KEY", "")        # 서버 PACKING_INGEST_KEY와 동일(안 쓰면 빈값)
    _fin_user = os.getenv("FINANCE_BASIC_USER", "")       # Nginx Basic 인증(직원 접속 계정)
    _fin_pass = os.getenv("FINANCE_BASIC_PASS", "")
    if _fin_url:
        print("📦 재무 ERP로 담당자 작업기록 전송 시작...")

        def _fin_key_norm(value):
            text = unicodedata.normalize("NFKC", str(value or ""))
            text = text.replace("﻿", "").replace("​", "")
            return re.sub(r"[\s_\-]+", "", text).lower()

        def _fin_col(*candidates):
            by_key = {_fin_key_norm(col): col for col in df_bq.columns}
            for cand in candidates:
                actual = by_key.get(_fin_key_norm(cand))
                if actual is not None:
                    return actual
            return ""

        def _fin_text(v):
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            return "" if s.lower() in ("nan", "none", "nat") else s

        _c_ap  = _fin_col("담당자1")
        _c_apd = _fin_col("승인일")
        _c_ar  = _fin_col("담당자2")
        _c_ard = _fin_col("도착일")
        if not (_c_ap and _c_ar):
            print(f"⚠️ 담당자 컬럼 없음 (담당자1={_c_ap!r}, 담당자2={_c_ar!r}) → 재무 전송 건너뜀")
        else:
            _fin_items = []
            for _, _r in df_bq.iterrows():
                _rd = _r.to_dict()
                _ap = _fin_text(_rd.get(_c_ap, ""))
                _ar = _fin_text(_rd.get(_c_ar, ""))
                if not _ap and not _ar:
                    continue  # 담당자 없는 행(아직 작업 안 된 아이템)은 건너뜀
                _fin_items.append({
                    "approver":     _ap,
                    "approve_date": _fin_text(_rd.get(_c_apd, "")),
                    "arriver":      _ar,
                    "arrive_date":  _fin_text(_rd.get(_c_ard, "")),
                })
            print(f"[INFO] 재무 전송 아이템: {len(_fin_items):,}건")
            _auth = (_fin_user, _fin_pass) if _fin_user else None
            _fin_resp = requests.post(
                f"{_fin_url}?k={_fin_key}",
                json={"items": _fin_items},
                auth=_auth,
                timeout=120,
            )
            if _fin_resp.status_code == 200:
                print(f"✅ 재무 ERP 전송 완료: {_fin_resp.json()}")
            else:
                print(f"❌ 재무 ERP 전송 실패: {_fin_resp.status_code} {_fin_resp.text[:200]}")
    else:
        print("[INFO] FINANCE_INGEST_URL 미설정 → 재무 전송 건너뜀")
except Exception as _e:
    print(f"❌ 재무 ERP 전송 중 오류(무시): {type(_e).__name__}: {_e}")


# =====================================================================
# 📦 [추가] 재무회계 ERP로 raw data 전체 전송 (금액대조·송금이익 + 근태)
#   위 packing/finance 블록과 별개. 금액·환율 포함 전체 행을 보낸다(같은 30분 주기).
#   서버가 rawitem(금액) + packing(근태)을 둘 다 만든다. try/except라 재무 서버 문제가
#   패킹 파이프라인에 영향 없음(엔드포인트 완전 분리).
# =====================================================================
try:
    _rd_url = os.getenv("FINANCE_RAWDATA_URL")           # 예: http://<서버IP>:8080/api/rawdata/ingest
    _rd_key = os.getenv("FINANCE_RAWDATA_KEY", "")       # 서버 RAWDATA_INGEST_KEY와 동일(안 쓰면 빈값)
    _rd_user = os.getenv("FINANCE_BASIC_USER", "")       # Nginx Basic 인증(패킹 블록과 동일 계정)
    _rd_pass = os.getenv("FINANCE_BASIC_PASS", "")
    if _rd_url:
        print("📦 재무 ERP로 raw data 전체 전송 시작...")

        def _rd_norm(value):
            text = unicodedata.normalize("NFKC", str(value or ""))
            text = text.replace("﻿", "").replace("​", "")
            return re.sub(r"[\s_\-]+", "", text).lower()

        def _rd_col(*candidates):
            by_key = {_rd_norm(col): col for col in df_bq.columns}
            for cand in candidates:
                actual = by_key.get(_rd_norm(cand))
                if actual is not None:
                    return actual
            return ""

        def _rd_text(v):
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            return "" if s.lower() in ("nan", "none", "nat") else s

        _c_item = _rd_col("아이템번호")
        _c_tot  = _rd_col("합계_원화_", "합계원화", "합계(원화)")
        _c_unit = _rd_col("단가_원화_", "단가원화", "단가(원화)")
        _c_qty  = _rd_col("최초_주문수량", "최초주문수량")   # ⚠️ 첫 실행 후 금액대조 빵꾸 수로 정상 여부 확인
        _c_fee  = _rd_col("수수료_원화_", "수수료원화", "수수료(원화)")
        _c_ship = _rd_col("현지배송비_원화_", "현지배송비원화", "현지배송비(원화)")
        _c_etc  = _rd_col("기타금액_원화_", "기타금액원화", "기타금액(원화)")
        _c_stat = _rd_col("주문상태")
        _c_fx   = _rd_col("환율")
        _c_ap   = _rd_col("담당자1")
        _c_apd  = _rd_col("승인일")
        _c_ar   = _rd_col("담당자2")
        _c_ard  = _rd_col("도착일")
        if not _c_item:
            print("⚠️ 아이템번호 컬럼 없음 → raw data 전송 건너뜀")
        else:
            _rd_rows = []
            for _, _r in df_bq.iterrows():
                _rd = _r.to_dict()
                _no = _rd_text(_rd.get(_c_item, ""))
                if not _no:
                    continue
                _rd_rows.append({
                    "item_no":      _no,
                    "total_krw":    _rd_text(_rd.get(_c_tot, "")),
                    "unit_krw":     _rd_text(_rd.get(_c_unit, "")),
                    "init_qty":     _rd_text(_rd.get(_c_qty, "")),
                    "fee_krw":      _rd_text(_rd.get(_c_fee, "")),
                    "ship_krw":     _rd_text(_rd.get(_c_ship, "")),
                    "etc_krw":      _rd_text(_rd.get(_c_etc, "")),
                    "status":       _rd_text(_rd.get(_c_stat, "")),
                    "fx":           _rd_text(_rd.get(_c_fx, "")),
                    "approver":     _rd_text(_rd.get(_c_ap, "")),
                    "approve_date": _rd_text(_rd.get(_c_apd, "")),
                    "arriver":      _rd_text(_rd.get(_c_ar, "")),
                    "arrive_date":  _rd_text(_rd.get(_c_ard, "")),
                })
            print(f"[INFO] raw data 전송 행: {len(_rd_rows):,}건")
            _rd_auth = (_rd_user, _rd_pass) if _rd_user else None
            _rd_resp = requests.post(
                f"{_rd_url}?k={_rd_key}",
                json={"rows": _rd_rows},   # mode 생략 = full(rawitem + packing 둘 다)
                auth=_rd_auth,
                timeout=120,
            )
            if _rd_resp.status_code == 200:
                print(f"✅ 재무 raw data 전송 완료: {_rd_resp.json()}")
            else:
                print(f"❌ 재무 raw data 전송 실패: {_rd_resp.status_code} {_rd_resp.text[:200]}")
    else:
        print("[INFO] FINANCE_RAWDATA_URL 미설정 → raw data 전송 건너뜀")
except Exception as _e:
    print(f"❌ 재무 raw data 전송 중 오류(무시): {type(_e).__name__}: {_e}")
