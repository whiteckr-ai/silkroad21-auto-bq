from __future__ import annotations

# =====================================================================
# 결제내역 · 예치금 크롤링 → 재무회계 ERP 직접 전송
#
#   silkroad21 관리자에서 결제내역(fnPageExl('Pmt')) · 예치금(fnPageExl('DpstDet'))
#   두 .xls를 받아, **파일을 그대로(gzip+base64)** 재무 ERP로 POST 한다.
#   서버가 클라 업로드와 똑같은 파서(parsePayEnd)로 파싱해 payment·deposit upsert.
#   → py는 다운로드만, 파싱·집계는 서버. 로직 중복 0, 수동 업로드와 동일 결과.
#
#   (구버전 auto_download_payment_to_sheets.py의 Google Sheets/BigQuery 전송은 전부 제거.
#    이제 재무 ERP로만 쏜다.)
# =====================================================================

import os
import sys
import time
import glob
import gzip
import base64
from pathlib import Path
from datetime import datetime

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import urllib3.exceptions

RETRYABLE_ERRORS = (
    TimeoutException,
    WebDriverException,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.ProtocolError,
    TimeoutError,
    ConnectionError,
)


# ===== 로그를 파일로도 남김 =====
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


sys.stdout = sys.stderr = DualLogger("log_payment.txt")

# ===== 환경 / 설정 =====
RUNNER = os.getenv("GITHUB_ACTIONS") == "true"

# 로그인 (raw data 파이프라인과 동일 계정)
LOGIN_ID = os.environ["LOGIN_ID"]
LOGIN_PW = os.environ["LOGIN_PW"]

# 재무 ERP 수신 (필수)
FIN_URL = os.environ["FINANCE_PAYMENT_URL"]          # 예: http://<서버IP>:8080/api/payment/ingest
FIN_KEY = os.getenv("FINANCE_PAYMENT_KEY", "")       # 서버 PAYMENT_INGEST_KEY와 동일(안 쓰면 빈값)
FIN_USER = os.getenv("FINANCE_BASIC_USER", "")       # Nginx Basic 인증(직원 접속 계정)
FIN_PASS = os.getenv("FINANCE_BASIC_PASS", "")

# 날짜 범위: 시작일 고정(기본 2026-01-01), 종료일은 실행 시점 오늘. 전체 범위를 매번 보내도
# 서버가 결제번호로 upsert 하므로 idempotent(중복/누락 없음).
START_DATE = os.getenv("PAY_START_DATE") or "2026-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# URLs
LOGIN_URL = "https://silkroad21.co.kr/pzadm/Login.asp"
# 결제내역: 다운로드 버튼 onclick=fnPageExl('Pmt')
PAYMENT_URL = os.getenv(
    "PAYMENT_PAGE_URL",
    "https://silkroad21.co.kr/Admin/Acting/Pay_End_Pmt_S.asp?shTbTy=PMT&gMnu1=101&gMnu2=10105",
)
# 예치금: 결제내역과 같은 페이지(Pay_End_Pmt_S.asp)의 DPST 탭. 다운로드 버튼 onclick=fnPageExl('DpstDet').
#   (2026-08-11 실제 주소 확인 완료. 바뀌면 env PAYMENT_DEPOSIT_URL 로 덮으면 됨.)
DEPOSIT_URL = os.getenv(
    "PAYMENT_DEPOSIT_URL",
    "https://silkroad21.co.kr/Admin/Acting/Pay_End_Pmt_S.asp?shTbTy=DPST&gMnu1=101&gMnu2=10105",
)

# 다운로드 폴더
if RUNNER:
    downloads_folder = str((Path.cwd() / "downloads_payment").resolve())
else:
    downloads_folder = r"C:\Users\white\Downloads\csv_payment"
Path(downloads_folder).mkdir(parents=True, exist_ok=True)


# ===== Helpers (구 스크립트와 동일한 로그인/이동 프레임워크) =====
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
                id_el.clear(); id_el.send_keys(LOGIN_ID)
                pw_el.clear(); pw_el.send_keys(LOGIN_PW)
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
                time.sleep(15 * attempt)
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
                time.sleep(15 * attempt)
    raise RuntimeError(f"페이지 이동 {max_retries}회 모두 실패. 마지막 에러: {last_error}")


def clear_downloads() -> None:
    """다운로드 폴더의 데이터 파일을 비운다(두 파일을 번갈아 받으니 매번 정리)."""
    for ext in ("*.csv", "*.xls", "*.xlsx", "*.crdownload"):
        for fp in glob.glob(os.path.join(downloads_folder, ext)):
            try:
                os.remove(fp)
            except Exception:
                pass


def wait_for_download_complete(timeout: int = 300) -> str:
    end = time.time() + timeout
    while time.time() < end:
        if glob.glob(os.path.join(downloads_folder, "*.crdownload")):
            time.sleep(0.8)
            continue
        done = []
        for ext in ("*.xls", "*.xlsx", "*.csv"):
            done.extend(glob.glob(os.path.join(downloads_folder, ext)))
        if done:
            return max(done, key=os.path.getctime)
        time.sleep(0.8)
    print(f"[DEBUG] 다운로드 폴더: {os.listdir(downloads_folder)}")
    raise TimeoutError("다운로드 완료 대기 시간 초과")


def set_easyui_datebox(driver: webdriver.Chrome, element_id: str, value: str) -> None:
    driver.execute_script("$(arguments[0]).datebox('setValue', arguments[1]);", f"#{element_id}", value)


def apply_search_filters(driver: webdriver.Chrome) -> None:
    """결제일(B) 기준 + 시작/종료일 지정 후 검색. 결제여부는 전체(상태 필터는 안 함 —
    서버가 전체 행을 받아 발행/제외를 판단하므로 결제대기·취소까지 다 보낸다).
    예치금 페이지엔 일부 컨트롤이 없을 수 있어 각각 try로 감싼다."""
    wait = WebDriverWait(driver, 20)

    ins_date_el = None
    try:
        ins_date_el = wait.until(EC.presence_of_element_located((By.ID, "shInsDate")))
        Select(ins_date_el).select_by_value("B")
        print("[INFO] 검색조건: 결제일(B) 기준")
    except Exception as e:
        print(f"[WARN] shInsDate 없음/설정 실패(예치금 페이지일 수 있음): {e}")

    try:
        set_easyui_datebox(driver, "shBeginDay", START_DATE)
        set_easyui_datebox(driver, "shEndDay", END_DATE)
        print(f"[INFO] 날짜 범위: {START_DATE} ~ {END_DATE}")
    except Exception as e:
        print(f"[WARN] 날짜 설정 실패: {e}")

    try:
        pmt_stat_el = driver.find_element(By.ID, "shPmtStat")
        Select(pmt_stat_el).select_by_value("")
        print("[INFO] 결제여부: 전체")
    except Exception:
        pass  # 예치금 페이지엔 없을 수 있음

    try:
        search_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[onclick*=\"fnPageMv('frmSearch'\"]"))
        )
        search_btn.click()
        print("[INFO] 검색 버튼 클릭")
        if ins_date_el is not None:
            try:
                WebDriverWait(driver, 20).until(EC.staleness_of(ins_date_el))
            except TimeoutException:
                time.sleep(2)
        else:
            time.sleep(2)
    except Exception as e:
        print(f"[WARN] 검색 버튼 클릭 실패, 고정 대기: {e}")
        time.sleep(2)
    time.sleep(1)  # datebox 위젯 재초기화 대기


def click_export(driver: webdriver.Chrome, fn_arg: str) -> None:
    """엑셀 다운로드 버튼(fnPageExl('<fn_arg>')) 클릭. 실패 시 스크립트 직접 호출로 대체."""
    try:
        wait = WebDriverWait(driver, 20)
        btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"button[onclick*=\"fnPageExl('{fn_arg}')\"]"))
        )
        print(f"[INFO] 엑셀 다운로드 클릭: fnPageExl('{fn_arg}')")
        btn.click()
    except Exception as e:
        print(f"[WARN] 버튼 클릭 실패 → execute_script 대체: {e}")
        driver.set_script_timeout(10)
        driver.execute_script(f"fnPageExl('{fn_arg}');")
    accept_alert_safe(driver, timeout=5)


def download_file(driver: webdriver.Chrome, page_url: str, fn_arg: str, label: str) -> str:
    """페이지 이동 → 검색 필터 → 엑셀 다운로드 → 받은 파일 경로 반환."""
    print(f"\n===== [{label}] 다운로드 시작 =====")
    clear_downloads()
    goto_with_auth(driver, page_url)
    apply_search_filters(driver)
    click_export(driver, fn_arg)
    path = wait_for_download_complete(timeout=300)
    size = os.path.getsize(path)
    print(f"[INFO] [{label}] 다운로드 완료: {os.path.basename(path)} ({size/1e6:.2f} MB)")
    return path


def post_to_finance(path: str, label: str) -> None:
    """받은 .xls를 gzip+base64 로 재무 ERP에 POST. 파싱은 서버가 parsePayEnd로."""
    with open(path, "rb") as f:
        raw = f.read()
    gz_b64 = base64.b64encode(gzip.compress(raw)).decode("ascii")
    print(f"[INFO] [{label}] 전송: 원본 {len(raw)/1e6:.2f}MB → gzip {len(gz_b64)*3/4/1e6:.2f}MB(b64)")
    auth = (FIN_USER, FIN_PASS) if FIN_USER else None
    resp = requests.post(
        f"{FIN_URL}?k={FIN_KEY}",
        json={"file_gz_b64": gz_b64, "filename": os.path.basename(path)},
        auth=auth,
        timeout=120,
    )
    if resp.status_code == 200:
        print(f"✅ [{label}] 재무 ERP 전송 완료: {resp.json()}")
    else:
        print(f"❌ [{label}] 재무 ERP 전송 실패: {resp.status_code} {resp.text[:300]}")
        raise RuntimeError(f"[{label}] 전송 실패 {resp.status_code}")


# ===== Main =====
def main() -> None:
    driver = make_driver(headless=True)
    results = []
    try:
        do_login(driver)
        for page_url, fn_arg, label in (
            (PAYMENT_URL, "Pmt", "결제내역"),
            (DEPOSIT_URL, "DpstDet", "예치금"),
        ):
            try:
                path = download_file(driver, page_url, fn_arg, label)
                post_to_finance(path, label)
                results.append((label, True))
            except Exception as e:
                import traceback
                print(f"❌ [{label}] 실패: {type(e).__name__}: {e}")
                traceback.print_exc()
                results.append((label, False))
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    ok = [l for l, s in results if s]
    bad = [l for l, s in results if not s]
    print(f"\n🎉 완료 — 성공: {ok or '없음'} / 실패: {bad or '없음'}")
    if bad:
        sys.exit(1)  # 하나라도 실패하면 워크플로우를 실패로 표시(추적용)


if __name__ == "__main__":
    main()
