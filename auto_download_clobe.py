from __future__ import annotations

# =====================================================================
# 클로브(app.clobe.ai) 통장 내역 크롤링 → 재무회계 ERP 직접 전송
#
#   클로브 통장 내역 화면의 「엑셀 다운로드」(시트 「통합 라벨링 내역」) .xlsx를 받아,
#   **파일을 그대로(gzip+base64)** 재무 ERP `/api/bank/ingest`로 POST 한다.
#   서버가 클라 업로드와 똑같은 파서(parseBankTx)로 파싱해 kind `bank` upsert.
#   → py는 다운로드만, 파싱은 서버. 로직 중복 0, 수동 업로드와 동일 결과.
#
#   ⚠️ 클로브는 SPA(React)라 silkroad21(ASP)과 달리 요소를 텍스트 기반 XPath로 잡고
#      hydration을 넉넉히 기다린다. 클래스명은 빌드마다 바뀔 수 있어 쓰지 않는다.
#
#   날짜: 화면 기본 조회범위를 그대로 받는다. BankTx.id가 내용 기반(계좌·일시·금액·적요)
#        멱등 키라 매일 겹쳐 받아도 upsert로 중복 없이 누적된다. 통장 파일이 작아(수천 행)
#        기본 범위를 매일 받아도 부담이 없다 → 깨지기 쉬운 날짜 picker 조작을 피한다.
# =====================================================================

import os
import sys
import time
import glob
import gzip
import base64
from pathlib import Path

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
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


sys.stdout = sys.stderr = DualLogger("log_clobe.txt")

# ===== 환경 / 설정 =====
RUNNER = os.getenv("GITHUB_ACTIONS") == "true"

# 클로브 로그인
LOGIN_ID = os.environ["CLOBE_LOGIN_ID"]     # 클로브 이메일
LOGIN_PW = os.environ["CLOBE_LOGIN_PW"]     # 클로브 비번

# 로그인 후 반드시 이 회사(워크스페이스)로 전환한다. 계정에 회사가 2개라 안 고르면
# 엉뚱한 데이터를 받는다. 이름 일부만 맞으면 됨(부분 일치).
COMPANY_NAME = os.getenv("CLOBE_COMPANY_NAME", "에스앤피그룹")

# 재무 ERP 수신 (필수)
FIN_URL = os.environ["FINANCE_BANK_URL"]         # 예: http://<서버IP>:8080/api/bank/ingest
FIN_KEY = os.getenv("FINANCE_BANK_KEY", "")      # 서버 BANK_INGEST_KEY와 동일(안 쓰면 빈값)
FIN_USER = os.getenv("FINANCE_BASIC_USER", "")   # Nginx Basic 인증(직원 접속 계정)
FIN_PASS = os.getenv("FINANCE_BASIC_PASS", "")

# URLs
SIGNIN_URL = "https://app.clobe.ai/auth/signin"
TRANSACTIONS_URL = "https://app.clobe.ai/clobe/transactions"

# 다운로드 폴더
if RUNNER:
    downloads_folder = str((Path.cwd() / "downloads_clobe").resolve())
else:
    downloads_folder = r"C:\Users\white\Downloads\csv_clobe"
Path(downloads_folder).mkdir(parents=True, exist_ok=True)


# ===== Helpers =====
def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--remote-allow-origins=*")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": downloads_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    chrome_bin = os.getenv("CHROME_PATH")
    if chrome_bin:
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(60)
    driver.set_page_load_timeout(180)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": downloads_folder},
        )
    except Exception:
        pass
    return driver


def do_login(driver: webdriver.Chrome, max_retries: int = 3) -> None:
    """클로브 이메일 로그인. SPA라 입력칸 hydration을 기다린다."""
    wait = WebDriverWait(driver, 30)
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] 로그인 시도 {attempt}/{max_retries}")
            driver.get(SIGNIN_URL)
            email_el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='email']")))
            pw_el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']")))
            email_el.clear(); email_el.send_keys(LOGIN_ID)
            pw_el.clear(); pw_el.send_keys(LOGIN_PW)
            # 제출 버튼(type=submit). 없으면 엔터로 대체.
            try:
                driver.find_element(By.CSS_SELECTOR, "form button[type='submit'], button[type='submit']").click()
            except Exception:
                pw_el.send_keys(Keys.RETURN)

            # 로그인 성공 = /auth 를 벗어남
            wait.until(lambda d: "/auth" not in d.current_url)
            print(f"[INFO] 로그인 성공 → {driver.current_url}")
            time.sleep(2)  # 홈 hydration
            return
        except RETRYABLE_ERRORS as e:
            last_error = e
            print(f"[WARN] 로그인 시도 {attempt} 실패: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                time.sleep(10 * attempt)
    raise RuntimeError(f"로그인 {max_retries}회 모두 실패. 마지막 에러: {last_error}")


def select_company(driver: webdriver.Chrome) -> None:
    """좌상단 워크스페이스 선택기를 열어 COMPANY_NAME 회사로 전환.
    이미 그 회사면 그대로 둔다. 계정에 회사가 2개라 이 단계가 필수."""
    wait = WebDriverWait(driver, 20)
    # 이미 헤더에 회사명이 보이면 전환 불필요일 수 있으나, 확실히 하려고 항상 시도한다.
    try:
        # 좌상단 워크스페이스 드롭다운(회사명이 들어간 버튼)을 연다.
        switcher = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[.//*[contains(text(),'주식회사') or contains(text(),'그룹')]]")
            )
        )
        switcher.click()
        time.sleep(1)
        # 드롭다운에서 COMPANY_NAME 항목 클릭
        target = wait.until(
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(),'{COMPANY_NAME}')]"))
        )
        target.click()
        print(f"[INFO] 회사 전환: {COMPANY_NAME}")
        time.sleep(2)
    except Exception as e:
        # 이미 그 회사가 선택돼 있거나 UI가 달라졌을 수 있다. 헤더에 회사명이 있으면 통과.
        print(f"[WARN] 회사 선택기 조작 실패({type(e).__name__}: {str(e)[:150]}). "
              f"현재 화면에 '{COMPANY_NAME}' 있는지 확인.")
        if COMPANY_NAME not in driver.page_source:
            raise RuntimeError(f"회사 '{COMPANY_NAME}' 전환 확인 실패 — 다른 회사 데이터를 받을 위험")


def wait_for_download_complete(timeout: int = 180) -> str:
    end = time.time() + timeout
    while time.time() < end:
        if glob.glob(os.path.join(downloads_folder, "*.crdownload")):
            time.sleep(0.8)
            continue
        done = []
        for ext in ("*.xlsx", "*.xls"):
            done.extend(glob.glob(os.path.join(downloads_folder, ext)))
        if done:
            return max(done, key=os.path.getctime)
        time.sleep(0.8)
    print(f"[DEBUG] 다운로드 폴더: {os.listdir(downloads_folder)}")
    raise TimeoutError("다운로드 완료 대기 시간 초과")


def clear_downloads() -> None:
    for ext in ("*.xlsx", "*.xls", "*.crdownload"):
        for fp in glob.glob(os.path.join(downloads_folder, ext)):
            try:
                os.remove(fp)
            except Exception:
                pass


def download_transactions(driver: webdriver.Chrome) -> str:
    """통장 내역 화면 → 「엑셀 다운로드」 클릭 → 파일 경로 반환.
    「위하고 업로드용 엑셀 다운로드」가 아니라 순수 「엑셀 다운로드」를 눌러야
    시트 「통합 라벨링 내역」이 들어온 파일이 온다(parseBankTx가 읽는 포맷)."""
    wait = WebDriverWait(driver, 30)
    clear_downloads()
    driver.get(TRANSACTIONS_URL)
    # 표(그리드)가 로드될 때까지 대기 = 데이터 준비됨
    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'통장 내역')]")))
    time.sleep(3)  # 데이터/버튼 hydration

    # 「엑셀 다운로드」 버튼 — '위하고'가 들어간 버튼은 제외.
    btn = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(normalize-space(.),'엑셀 다운로드') and not(contains(.,'위하고'))]")
        )
    )
    print("[INFO] 「엑셀 다운로드」 클릭")
    btn.click()

    # 다운로드 옵션 모달/확인이 뜨는 경우 대비: '다운로드'/'확인' 버튼이 있으면 누른다.
    try:
        confirm = WebDriverWait(driver, 4).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(.)='다운로드' or normalize-space(.)='확인']")
            )
        )
        confirm.click()
        print("[INFO] 다운로드 확인 모달 처리")
    except Exception:
        pass

    path = wait_for_download_complete(timeout=180)
    size = os.path.getsize(path)
    print(f"[INFO] 통장 내역 다운로드 완료: {os.path.basename(path)} ({size/1e6:.2f} MB)")
    return path


def post_to_finance(path: str) -> None:
    """받은 .xlsx를 gzip+base64 로 재무 ERP에 POST. 파싱은 서버가 parseBankTx로."""
    with open(path, "rb") as f:
        raw = f.read()
    gz_b64 = base64.b64encode(gzip.compress(raw)).decode("ascii")
    print(f"[INFO] 전송: 원본 {len(raw)/1e6:.2f}MB → gzip {len(gz_b64)*3/4/1e6:.2f}MB(b64)")
    auth = (FIN_USER, FIN_PASS) if FIN_USER else None
    resp = requests.post(
        f"{FIN_URL}?k={FIN_KEY}",
        json={"file_gz_b64": gz_b64, "filename": os.path.basename(path)},
        auth=auth,
        timeout=120,
    )
    if resp.status_code == 200:
        print(f"✅ 재무 ERP 전송 완료: {resp.json()}")
    else:
        print(f"❌ 재무 ERP 전송 실패: {resp.status_code} {resp.text[:300]}")
        raise RuntimeError(f"전송 실패 {resp.status_code}")


# ===== Main =====
def main() -> None:
    driver = make_driver(headless=True)
    try:
        do_login(driver)
        select_company(driver)
        path = download_transactions(driver)
        post_to_finance(path)
        print("\n🎉 완료 — 통장 내역 전송 성공")
    except Exception as e:
        import traceback
        print(f"❌ 실패: {type(e).__name__}: {e}")
        traceback.print_exc()
        # 실패 화면 스크린샷(디버그용)
        try:
            driver.save_screenshot(os.path.join(downloads_folder, "error.png"))
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
