# dart_api_collector_v3.py
# 실행 전: pip install requests pandas tqdm
# config.py 내부에 api_key 변수로 OpenDART API 키가 있어야 합니다.
# 예: api_key = "본인_OpenDART_API_KEY"

import os
import time
import requests
import json
import pandas as pd
from tqdm import tqdm
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import config

# ----------------------- 설정 영역 -----------------------
API_KEY = config.api_key
BASE_SAVE_DIR = r"C:\Users\User\OneDrive\문서\개발100제\total_finance_crawling\naver_news_crawler\api_dart_crawler"
PROCESSED_LOG_FILE = os.path.join(BASE_SAVE_DIR, "processed_companies.log")
os.makedirs(BASE_SAVE_DIR, exist_ok=True)

# ⭐ 배치 실행 설정 ⭐
BATCH_SIZE = 400
START_INDEX = 0
END_INDEX = None

# 디버깅 모드
DEBUG_MODE = True

# 기간 설정
current_year = datetime.now().year
start_year = current_year - 10
end_year = current_year

print(f"수집 기간: {start_year}년 ~ {end_year}년")

# OpenDART API 엔드포인트
API_CORP_CODE = "https://opendart.fss.or.kr/api/corpCode.xml"
API_FNLTT_ALL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

# 보고서 코드 우선순위
REPRT_PRIORITY = [('11013', '1분기보고서' ), ('11012', '반기보고서'), ('11014', '3분기보고서'), ('11011', '사업보고서')]
REPORT_DATE_MAP = {'11013': '03-31', '11012': '06-30', '11014': '09-30', '11011': '12-31'}

# 계정 및 키워드 매핑 (기존과 동일)
account_map = {
    '자산총계': ['ifrs-full_Assets', 'dart_TotalAssets', 'ifrs_Assets', 'Assets'],
    '부채총계': ['ifrs-full_Liabilities', 'dart_TotalLiabilities', 'ifrs_Liabilities', 'Liabilities'],
    '자본총계': ['ifrs-full_Equity', 'dart_TotalEquity', 'ifrs_Equity', 'Equity'],
    '매출액': ['ifrs-full_Revenue', 'dart_Revenue', 'Revenue'],
    '당기순이익': ['ifrs-full_ProfitLoss', 'dart_ProfitLoss', 'ProfitLoss']
}
keyword_map = {
    '자산총계': ['자산총계', '총자산'], '부채총계': ['부채총계', '총부채'], '자본총계': ['자본총계', '총자본'],
    '매출액': ['매출액', '영업수익'], '당기순이익': ['당기순이익', '순이익']
}

# ----------------------- 유틸 함수들 -----------------------
def debug_log(message, level="INFO"):
    if DEBUG_MODE:
        print(f"    [{level}] {datetime.now().strftime('%H:%M:%S')} - {message}")

def safe_parse_num(x):
    if x is None: return None
    s = str(x).strip().replace(',', '')
    if not s or s.lower() in ['nan', 'none', 'null', '-']: return None
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    try: return float(s)
    except (ValueError, TypeError): return None

# ⭐ [수정됨] 상장폐지 기업 필터링 로직 추가
def fetch_all_listed_companies(api_key, timeout=60):
    params = {'crtfc_key': api_key}
    try:
        print("🌐 OpenDART에서 전체 기업 목록을 가져오는 중...")
        r = requests.get(API_CORP_CODE, params=params, timeout=timeout)
        r.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            xml_filename = z.namelist()[0]
            with z.open(xml_filename) as f:
                root = ET.fromstring(f.read())
        
        listed_companies = {}
        total_corps = 0
        delisted_corps = 0
        # 상장 폐지 필터링 기준 날짜 (현재로부터 약 2년 전)
        delist_cutoff_date = datetime.now() - timedelta(days=365 * 2)

        for corp in root.findall('list'):
            total_corps += 1
            stock_code_node = corp.find('stock_code')
            # 종목 코드가 없는 기업(비상장 등)은 제외
            if stock_code_node is None or not stock_code_node.text.strip():
                continue

            # 상장 폐지일(modify_date) 확인
            modify_date_str = corp.find('modify_date').text
            if modify_date_str:
                try:
                    modify_date = datetime.strptime(modify_date_str, '%Y%m%d')
                    # 상장 폐지일이 2년보다 더 오래되었으면 건너뜀
                    if modify_date < delist_cutoff_date:
                        delisted_corps += 1
                        continue
                except ValueError:
                    pass # 날짜 형식이 잘못된 경우 무시

            stock_code = stock_code_node.text.strip()
            if len(stock_code) == 6 and stock_code.isdigit():
                listed_companies[stock_code] = {
                    'corp_code': corp.find('corp_code').text,
                    'corp_name': corp.find('corp_name').text,
                    'stock_code': stock_code
                }
        
        print(f"✅ 전체 기업 {total_corps}개 중, 오래전 상장폐지된 기업 {delisted_corps}개를 제외하고 유효한 {len(listed_companies)}개를 확인했습니다.")
        return listed_companies
        
    except Exception as e:
        print(f"❌ 상장기업 목록 가져오기 실패: {e}")
        return {}

def fetch_fnltt_all(api_key, corp_code, bsns_year, reprt_code, fs_div='CFS', timeout=30):
    params = {'crtfc_key': api_key, 'corp_code': corp_code, 'bsns_year': str(bsns_year), 'reprt_code': reprt_code, 'fs_div': fs_div}
    debug_log(f"API 호출: {corp_code}, {bsns_year}, {reprt_code}, {fs_div}")
    try:
        r = requests.get(API_FNLTT_ALL, params=params, timeout=timeout)
        r.raise_for_status()
        result = r.json()
        if result.get('status') != '000':
            debug_log(f"⚠️ API 응답: status={result.get('status')}, message={result.get('message', '')}")
        return result
    except Exception as e:
        debug_log(f"❌ 요청 실패: {e}", "ERROR")
        return {'status': '999', 'message': f'request_failed:{e}'}

def extract_values_from_list(items_list, account_tags):
    if not items_list: return None
    for tag in account_tags:
        for item in items_list:
            if item.get('account_id', '').strip().lower() == tag.lower():
                return safe_parse_num(item.get('thstrm_amount'))
    return None

def find_by_account_name_keywords(items_list, keywords):
    if not items_list: return None
    for keyword in keywords:
        for item in items_list:
            if keyword in item.get('account_nm', ''):
                return safe_parse_num(item.get('thstrm_amount'))
    return None

# ----------------------- 핵심 로직 -----------------------

# ⭐ [수정됨] 조기 중단 로직 추가
def collect_company_data(api_key, corp_code, corp_name, stock_code):
    rows = []
    consecutive_failures = 0 # 연속 실패 횟수
    FAILURE_THRESHOLD = 3    # 연속 3회 실패 시 해당 기업 건너뛰기

    for year in range(end_year, start_year - 1, -1): # 최신 연도부터 탐색
        collected_reports_for_year = set()
        for reprt_code, report_name in REPRT_PRIORITY:
            if reprt_code in collected_reports_for_year: continue
            
            # 조기 중단 체크
            if consecutive_failures >= FAILURE_THRESHOLD:
                print(f"    ⚠️ 연속 {FAILURE_THRESHOLD}회 데이터 조회 실패. 이 기업의 남은 기간 조회를 중단합니다.")
                if not rows: return None
                else: return pd.DataFrame(rows)

            time.sleep(0.2)
            
            result = fetch_fnltt_all(api_key, corp_code, year, reprt_code, fs_div='CFS')
            if result.get('status') != '000' or not result.get('list'):
                debug_log(f"CFS 데이터 없음 ({year} {report_name}), OFS 시도...")
                result = fetch_fnltt_all(api_key, corp_code, year, reprt_code, fs_div='OFS')

            if result.get('status') == '000' and result.get('list'):
                consecutive_failures = 0 # 성공 시 초기화
                items = result['list']
                debug_log(f"✅ 데이터 발견: {year}년 {report_name} ({len(items)}개 계정)")
                
                row = {}
                found_any = False
                for label, tags in account_map.items():
                    value = extract_values_from_list(items, tags)
                    if value is None: value = find_by_account_name_keywords(items, keyword_map.get(label, []))
                    row[label] = value
                    if value is not None: found_any = True
                
                if found_any:
                    row.update({'bsns_year': year, 'reprt_code': reprt_code, 'report_name': report_name,
                                'report_date': f"{year}-{REPORT_DATE_MAP[reprt_code]}",
                                '기업명': corp_name, '종목코드': stock_code})
                    rows.append(row)
                    collected_reports_for_year.add(reprt_code)
            else:
                debug_log(f"데이터 없음: {year}년 {report_name}")
                consecutive_failures += 1

    if not rows:
        print(f"    ❌ 전체 기간에서 유의미한 데이터 수집 실패")
        return None
    
    print(f"    ✅ 총 {len(rows)}개 분기/반기/연간 보고서 데이터 수집 성공!")
    return pd.DataFrame(rows)

def save_company_data(corp_name, stock_code, df):
    if df is None or df.empty: return False
    safe_name = "".join(c for c in corp_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    file_name = f"dart_financial_{safe_name}_{stock_code}.csv"
    save_path = os.path.join(BASE_SAVE_DIR, file_name)
    try:
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"    💾 저장 완료: {file_name}")
        return True
    except Exception as e:
        print(f"    ❌ 저장 실패: {e}")
        return False

def load_processed_companies():
    if not os.path.exists(PROCESSED_LOG_FILE): return set()
    with open(PROCESSED_LOG_FILE, 'r', encoding='utf-8') as f:
        processed = {line.strip() for line in f}
    print(f"📖 이전에 처리된 기업 {len(processed)}개를 건너뜁니다.")
    return processed

def log_processed_company(stock_code):
    with open(PROCESSED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{stock_code}\n")

# ----------------------- 메인 실행 부분 -----------------------
def main():
    print("🚀 OpenDART 재무데이터 수집기 v3 (상장폐지 기업 필터링 및 조기 중단 기능 추가)")
    print("=" * 60)
    
    listed_companies_dict = fetch_all_listed_companies(API_KEY)
    if not listed_companies_dict: return

    processed_stock_codes = load_processed_companies()
    companies_to_process = [(code, info) for code, info in listed_companies_dict.items() if code not in processed_stock_codes]
    
    start = START_INDEX
    end = END_INDEX if END_INDEX is not None else len(companies_to_process)
    batch_companies = companies_to_process[start:end]
    if BATCH_SIZE > 0: batch_companies = batch_companies[:BATCH_SIZE]

    print(f"\n📊 배치 정보: 전체 {len(listed_companies_dict):,}개 중 미처리 {len(companies_to_process):,}개 / 이번 세션 처리 대상: {len(batch_companies)}개")
    print("-" * 60)

    if not batch_companies:
        print("🎉 모든 기업의 데이터 수집이 완료되었습니다!")
        return

    success_count = 0
    for stock_code, corp_info in tqdm(batch_companies, desc="기업 처리 중"):
        print(f"\n\n[처리 시작] {corp_info['corp_name']}({stock_code})")
        try:
            df = collect_company_data(API_KEY, corp_info['corp_code'], corp_info['corp_name'], stock_code)
            if df is not None and not df.empty:
                if save_company_data(corp_info['corp_name'], stock_code, df):
                    success_count += 1
            log_processed_company(stock_code)
        except Exception as e:
            print(f"  ❌ 처리 중 심각한 예외 발생: {e}")
            log_processed_company(stock_code)
    
    print("\n" + "=" * 60)
    print("🎉 배치 처리 완료!")
    print(f"📋 이번 세션에서 {success_count}개 기업의 데이터를 성공적으로 저장했습니다.")

if __name__ == "__main__":
    main()
