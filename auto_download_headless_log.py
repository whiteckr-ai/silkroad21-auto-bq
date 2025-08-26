from selenium.common.exceptions import UnexpectedAlertPresentException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, os, glob, pandas as pd, re
from google.cloud import bigquery
import sys
from pathlib import Path
import os
# 맨 위 설정 근처에 추가
LOGIN_ID = os.getenv("LOGIN_ID", LOGIN_ID)
LOGIN_PW = os.getenv("LOGIN_PW", LOGIN_PW)
PROJECT_ID = os.getenv("GCP_PROJECT", PROJECT_ID)
DATASET_ID = os.getenv("BQ_DATASET", DATASET_ID)
TABLE_ID   = os.getenv("BQ_TABLE", TABLE_ID)



# 안전한 알림 닫기 함수 추가 (어디든 함수 구역에)
def accept_alert_safe(driver, timeout=3):
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        txt = alert.text
        print("[ALERT]", txt)
        alert.accept()
        return True
    except Exception:
        return False


# 로그 파일로 표준 출력/에러 저장 (원본 유지)
sys.stdout = open("log.txt", "w", encoding="utf-8")
sys.stderr = sys.stdout

# ✅ 다운로드 경로 (보호폴더 이슈 피하려면 C:\work\dl\csv 같은 경로도 권장)
downloads_folder = r"C:\Users\white\Downloads\csv"
os.makedirs(downloads_folder, exist_ok=True)

# ✅ 크롬 옵션
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")   # 필요 시 헤드리스 ON
options.add_argument("--disable-gpu")
options.add_argument("--disable-features=RendererCodeIntegrity")  # 보안모듈 충돌 우회
options.add_argument("--disable-features=NetworkService")         # 네트워크 서비스 크래시 우회
options.add_argument("--remote-allow-origins=*")                  # 신버전 드라이버 호환
options.add_argument("--no-sandbox")                              # 샌드박스 권한 이슈 우회
options.add_experimental_option("prefs", {
    "download.default_directory": downloads_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
})

# ✅ 크롬 실행 (Selenium Manager: 드라이버 자동 관리)
driver = webdriver.Chrome(options=options)

# (일부 환경에서 헤드리스 다운로드 허용이 필요할 수 있음)
try:
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": downloads_folder}
    )
except Exception:
    pass

LOGIN_URL = "https://silkroad21.co.kr/pzadm/Login.asp"
LIST_URL  = "https://silkroad21.co.kr/Admin/Acting/Acting_S.asp?gMnu1=101&gMnu2=10101"

LOGIN_ID = "ppazic"   # ← 실제 계정
LOGIN_PW = "123123"   # ← 실제 비번

# ✅ 로그인 절차를 함수로 분리 (대기/백업 시도/알림창 처리까지 포함)
def do_login(driver):
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)

    # ID/PW 입력
    id_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemId")))
    pw_el = wait.until(EC.presence_of_element_located((By.NAME, "sMemPw")))
    try: id_el.clear()
    except: pass
    id_el.send_keys(LOGIN_ID)
    try: pw_el.clear()
    except: pass
    pw_el.send_keys(LOGIN_PW)
    pw_el.send_keys(Keys.RETURN)

    # 경고 뜨면 닫고 한 번 더 시도
    if accept_alert_safe(driver, timeout=3):
        # 다시 입력 후 제출
        id_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "sMemId")))
        pw_el = driver.find_element(By.NAME, "sMemPw")
        id_el.clear(); id_el.send_keys(LOGIN_ID)
        pw_el.clear(); pw_el.send_keys(LOGIN_PW); pw_el.send_keys(Keys.RETURN)
        accept_alert_safe(driver, timeout=2)

    # 로그인 성공 판정: 로그인 페이지가 아닌지/관리자 경로로 갔는지 확인
    try:
        wait.until(lambda d: "Login.asp" not in d.current_url)
    except TimeoutException:
        # 버튼/폼 submit 백업 시도
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            btn.click()
            WebDriverWait(driver, 10).until(lambda d: "Login.asp" not in d.current_url)
        except Exception:
            print("[LOGIN DEBUG] title =", driver.title)
            print("[LOGIN DEBUG] url   =", driver.current_url)
            raise RuntimeError("로그인에 실패했습니다. 계정/셀렉터 확인 필요")

# ✅ 다운로드 완료 대기 (.crdownload 사라질 때까지)
def wait_for_download_complete(dirpath: str, timeout: int = 180):
    end = time.time() + timeout
    while time.time() < end:
        if glob.glob(os.path.join(dirpath, "*.crdownload")):
            time.sleep(0.8)
            continue
        if glob.glob(os.path.join(dirpath, "*.csv")):
            return True
        time.sleep(0.8)
    raise TimeoutError("다운로드 완료 대기 시간 초과")

# “페이지 이동 + 세션 확인” 함수 추가
def goto_with_auth(driver, url, login_url_hint="Login.asp"):
    """보호 페이지로 이동했는데 로그인으로 튕기면 재로그인 후 다시 이동."""
    driver.get(url)
    time.sleep(0.5)
    # 로그인 페이지로 리다이렉트됐으면 재로그인
    if login_url_hint in driver.current_url:
        print("[INFO] 세션 만료로 재로그인 시도")
        do_login(driver)
        driver.get(url)


# === 메인 ===
# 1) 로그인
do_login(driver)

# 2) 다운로드 페이지 이동
driver.get(LIST_URL)
WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located)

# 3) 내보내기 트리거 (사이트 JS 호출)
driver.execute_script("fnPageExl('X14');")

# 4) 다운로드 완료 대기
wait_for_download_complete(downloads_folder, timeout=180)
driver.quit()

# 5) 최신 파일만 유지 (없을 때 가드)
csv_files = glob.glob(os.path.join(downloads_folder, "*.csv"))
if not csv_files:
    print("❌ CSV 파일이 존재하지 않습니다. (다운로드 실패)")
    sys.exit(1)

latest_file = max(csv_files, key=os.path.getctime)
for file in csv_files:
    if file != latest_file:
        os.remove(file)
        print(f"🗑 삭제됨: {os.path.basename(file)}")

# 6) Pandas로 안전하게 읽기 + 정제
def sanitize_columns(columns):
    seen = {}
    clean_cols = []
    for col in columns:
        col = (col or "").strip()
        col = re.sub(r"[^\w]", "_", col)
        if re.match(r"^\d", col):
            col = "_" + col
        base = col
        i = 1
        while col in seen:
            col = f"{base}_{i}"
            i += 1
        seen[col] = True
        clean_cols.append(col)
    return clean_cols

# 인코딩 폴백
try:
    df = pd.read_csv(latest_file, encoding="utf-8-sig", dtype=str, on_bad_lines="skip")
except Exception:
    df = pd.read_csv(latest_file, encoding="cp949", dtype=str, on_bad_lines="skip")

print(f"📊 데이터 로딩 완료: {len(df)} rows")
df.columns = sanitize_columns(df.columns)
df = df.dropna(how="all").drop_duplicates()
print("🧹 데이터 정제 완료")

# 7) BigQuery 업로드
base_dir = Path(__file__).parent
json_path = base_dir / "bigquery-credentials.json"  # 스크립트 폴더에 키 파일
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(json_path)

client = bigquery.Client()
table_id = "savvy-mantis-457008-k6.raw_data.goods_csv"

job = client.load_table_from_dataframe(
    df,
    table_id,
    location="asia-northeast3",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
)
job.result()
print(f"✅ BigQuery 업로드 성공: {len(df)}건 → {table_id}")
