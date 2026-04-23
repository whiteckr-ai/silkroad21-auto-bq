import os
import sys
import requests
import pandas as pd
from google.cloud import bigquery

print("🚀 BigQuery -> Supabase 동기화 프로세스 시작...")

# 1. 환경 변수 세팅
PROJECT_ID = os.getenv("GCP_PROJECT") or "savvy-mantis-457008-k6"
DATASET_ID = os.getenv("BQ_DATASET") or "raw_data"
TABLE_ID = os.getenv("BQ_TABLE") or "goods_csv"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE")

if not all([PROJECT_ID, DATASET_ID, TABLE_ID, SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE]):
    print("❌ 에러: 필수 환경 변수가 누락되었습니다. (URL 또는 KEY 확인 필요)")
    sys.exit(1)

# 2. BigQuery에서 데이터 가져오기
print(f"📥 BigQuery에서 데이터 다운로드 중... (`{DATASET_ID}.{TABLE_ID}`)")
try:
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    df = client.query(query).to_dataframe()
    
    # 💡 [핵심] BigQuery 데이터 안의 중복 '아이템번호' 제거 (최신 데이터 1개만 유지)
    if '아이템번호' in df.columns:
        df = df.drop_duplicates(subset=['아이템번호'], keep='last')
        print(f"🧹 중복 데이터 제거 완료. 남은 데이터: 총 {len(df)}건")

    # Pandas 결측치를 DB용 완벽한 None으로 1차 변환
    df = df.astype(object).where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    
    # 스페이스바 공백(" "), "nan" 문자열 등 모든 형태의 찌꺼기를 None으로 2차 확인 사살
    for row in records:
        for key, value in row.items():
            if isinstance(value, str):
                cleaned_val = value.strip()
                if cleaned_val in ["", "nan", "None", "<NA>", "NaT"]:
                    row[key] = None
                else:
                    row[key] = cleaned_val

    print(f"✅ 데이터 전처리 완료: 최종 전송 대기 {len(records)}건")
except Exception as e:
    print(f"❌ BigQuery 읽기 실패: {e}")
    sys.exit(1)

if not records:
    print("⚠️ BigQuery에 전송할 데이터가 없습니다.")
    sys.exit(0)

# 3. Supabase 전송 세팅
API_URL = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
auth_headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# 4. 기존 Supabase 데이터 싹 지우기
print("🗑️ 기존 Supabase 데이터 삭제 중...")
try:
    delete_url = f"{API_URL}?id=not.is.null" 
    requests.delete(delete_url, headers=auth_headers, timeout=60)
    print("✅ 기존 데이터 삭제 완료!")
except Exception as e:
    print(f"❌ 데이터 삭제 통신 에러: {e}")

# 5. Supabase로 새 데이터 밀어넣기
insert_headers = auth_headers.copy()
insert_headers["Content-Type"] = "application/json"
insert_headers["Prefer"] = "return=minimal"

chunk_size = 3000
total_chunks = (len(records) // chunk_size) + 1

for i in range(0, len(records), chunk_size):
    chunk = records[i : i + chunk_size]
    try:
        response = requests.post(API_URL, headers=insert_headers, json=chunk, timeout=60)
        current_chunk = (i // chunk_size) + 1
        
        if response.status_code in [200, 201, 204]:
            print(f"📡 [{current_chunk}/{total_chunks}회차] 전송 성공")
        else:
            print(f"❌ [{current_chunk}/{total_chunks}회차] 실패: {response.text}")
            
    except Exception as e:
        print(f"❌ 전송 중 통신 에러 발생: {e}")

print("🎉 BigQuery -> Supabase 동기화 완벽 종료!")
