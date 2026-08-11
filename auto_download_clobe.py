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

# 로그인 후 반드시 이 회사(워크스페이스)여야 한다. 계정에 회사가 2개라 다르면 엉뚱한 데이터를 받는다.
# ⚠️ os.getenv(key, default)는 yml이 빈 문자열을 넘기면(시크릿 미설정) default 대신 ""를 준다.
#    → `or`로 빈값도 기본값으로 떨어지게 한다(payment 예치금 URL과 같은 함정).
COMPANY_NAME = os.getenv("CLOBE_COMPANY_NAME") or "에스앤피그룹"

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
    # ⚠️ excludeSwitches / useAutomationExtension 은 넣지 말 것 — 이 조합이 로그인 폼 로드를
    #    막아 로그인 3회 타임아웃을 냈다(실측). AutomationControlled 만 남길 수도 있으나,
    #    안전하게 known-good 구성으로 되돌린다.
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


def dismiss_modals(driver: webdriver.Chrome) -> None:
    """로그인 직후 뜨는 안내 팝업(예: '서비스 개선 안내')을 닫는다. 클릭을 가로채므로 필수.
    ESC + 닫기(X) 버튼을 관대하게 시도한다(없으면 조용히 통과)."""
    for _ in range(3):
        closed = False
        try:
            driver.switch_to.active_element.send_keys(Keys.ESCAPE)
        except Exception:
            pass
        for xp in (
            "//button[@aria-label='닫기' or @aria-label='Close']",
            "//div[@role='dialog']//button[contains(.,'닫기')]",
        ):
            try:
                for el in driver.find_elements(By.XPATH, xp):
                    if el.is_displayed():
                        driver.execute_script("arguments[0].click();", el)
                        closed = True
                        time.sleep(0.4)
            except Exception:
                pass
        if not closed:
            break
        time.sleep(0.4)


def wait_for_data(driver: webdriver.Chrome, timeout: int = 40) -> None:
    """통장내역 표의 실데이터 로드를 기다린다. 사이드바 '통장 내역' 글자는 항상 있으니 쓰면 안 되고,
    데이터가 있어야만 뜨는 푸터 '합계'(입금액 합계/출금액 합계)를 기다린다."""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'합계')]"))
    )
    time.sleep(2)  # 버튼 hydration 여유


def verify_company(driver: webdriver.Chrome) -> None:
    """좌상단 워크스페이스가 COMPANY_NAME인지 확인하고, 다르면 전환한다(계정에 회사 2개).
    로그인 기본값이 보통 맞으므로 대개 확인만 하고 넘어간다."""
    wait = WebDriverWait(driver, 20)
    switcher = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//button[.//*[contains(text(),'주식회사') or contains(text(),'그룹')]]")
        )
    )
    if COMPANY_NAME in (switcher.text or ""):
        print(f"[INFO] 회사 확인 OK: {COMPANY_NAME} (전환 불필요)")
        return
    print(f"[INFO] 현재 회사가 '{COMPANY_NAME}' 아님(현재: {switcher.text!r}) → 전환 시도")
    dismiss_modals(driver)
    driver.execute_script("arguments[0].click();", switcher)  # 가로채임 우회: JS 클릭
    time.sleep(1)
    target = wait.until(
        EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(),'{COMPANY_NAME}')]"))
    )
    driver.execute_script("arguments[0].click();", target)
    print(f"[INFO] 회사 전환: {COMPANY_NAME}")
    time.sleep(3)
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
    """「엑셀 다운로드」 클릭 → 파일 경로 반환. (통장내역 페이지·데이터 로드는 호출 전에 끝나 있어야 함.)
    「위하고 업로드용」이 아니라 순수 「엑셀 다운로드」를 눌러야 시트 「통합 라벨링 내역」 파일이 온다."""
    wait = WebDriverWait(driver, 30)
    clear_downloads()

    # 「엑셀 다운로드」 버튼 — '위하고'가 들어간 버튼은 제외.
    btn = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(normalize-space(.),'엑셀 다운로드') and not(contains(.,'위하고'))]")
        )
    )
    print("[INFO] 「엑셀 다운로드」 클릭")
    try:
        btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", btn)

    # 다운로드 옵션 모달/확인이 뜨는 경우 대비: 대화상자 안 '다운로드'/'확인' 버튼을 누른다.
    try:
        confirm = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[@role='dialog']//button[contains(.,'다운로드') or normalize-space(.)='확인']")
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
        # 홈의 안내 모달을 피하려 통장내역으로 직행(회사 선택기는 상단바라 여기서도 됨).
        driver.get(TRANSACTIONS_URL)
        dismiss_modals(driver)
        wait_for_data(driver)
        verify_company(driver)
        wait_for_data(driver)  # 전환했을 수 있으니 데이터 재확인
        path = download_transactions(driver)
        post_to_finance(path)
        print("\n🎉 완료 — 통장 내역 전송 성공")
    except Exception as e:
        import traceback
        print(f"❌ 실패: {type(e).__name__}: {e}")
        traceback.print_exc()
        # 실패 화면 스크린샷 + page.html(디버그용)
        try:
            driver.save_screenshot(os.path.join(downloads_folder, "error.png"))
            with open(os.path.join(downloads_folder, "page.html"), "w", encoding="utf-8") as f:
                f.write(driver.page_source[:500000])
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
