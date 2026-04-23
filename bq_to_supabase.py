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
    print("❌ 에러: 필수 환경 변수가 누락되었습니다.")
    sys.exit(1)

# 2. BigQuery에서 데이터 가져오기
print(f"📥 BigQuery에서 데이터 다운로드 중... (`{DATASET_ID}.{TABLE_ID}`)")
try:
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    df = client.query(query).to_dataframe()
    
    # 중복 데이터 완벽 제거
    if '아이템번호' in df.columns:
        df['아이템번호'] = df['아이템번호'].astype(str).str.strip()
        df = df.drop_duplicates(subset=['아이템번호'], keep='last')

    # DB용 빈칸(None) 처리
    df = df.astype(object).where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    
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
    print("⚠️ 전송할 데이터가 없습니다.")
    sys.exit(0)

# 3. Supabase API 세팅
API_URL = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
auth_headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# 4. 🧹 [핵심] 완벽한 기존 데이터 삭제 (아이템번호 기준)
print("🗑️ 기존 Supabase 데이터 진짜 삭제 중...")
try:
    # 아이템번호가 존재하는 모든 줄을 삭제 = 전체 삭제
    delete_url = f"{API_URL}?아이템번호=not.is.null" 
    requests.delete(delete_url, headers=auth_headers, timeout=60)
    print("✅ 기존 데이터 삭제 완료!")
except Exception as e:
    print(f"❌ 데이터 삭제 통신 에러: {e}")

# 5. 🚀 [핵심] 완벽한 덮어쓰기(Upsert) 설정
insert_headers = auth_headers.copy()
insert_headers["Content-Type"] = "application/json"
insert_headers["Prefer"] = "return=minimal, resolution=merge-duplicates"

# 💡 명시적으로 "아이템번호가 겹치면 무조건 덮어써라" 라고 API에 타겟 지정
upsert_url = f"{API_URL}?on_conflict=아이템번호"

chunk_size = 3000
total_chunks = (len(records) // chunk_size) + 1

for i in range(0, len(records), chunk_size):
    chunk = records[i : i + chunk_size]
    try:
        response = requests.post(upsert_url, headers=insert_headers, json=chunk, timeout=60)
        current_chunk = (i // chunk_size) + 1
        
        if response.status_code in [200, 201, 204]:
            print(f"📡 [{current_chunk}/{total_chunks}회차] 덮어쓰기 전송 성공")
        else:
            print(f"❌ [{current_chunk}/{total_chunks}회차] 실패: {response.text}")
            
    except Exception as e:
        print(f"❌ 전송 중 통신 에러 발생: {e}")

print("🎉 BigQuery -> Supabase 동기화 완벽 종료!")
