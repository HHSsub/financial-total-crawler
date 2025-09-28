# krx_quarterly_fetch.py
"""
목적: DART기업목록.csv에 있는 상장기업(종목코드)에 대해
2014 ~ (가장 최근 완결된) 분기별(분기말) KRX 마켓 데이터(시가총액, 상장주식수, 종가 등)를 수집하여 CSV로 저장.
- 권장: pip install pykrx pandas tqdm
- 실행환경: 인터넷 연결된 파이썬 (pykrx는 KRX 사이트를 호출합니다)
"""

import re
import os
import time
import calendar
from datetime import date, datetime
import pandas as pd
from tqdm import tqdm

# -- pykrx (KRX 데이터) --
# 반드시 설치: pip install pykrx
from pykrx import stock

# ---------- 설정 ----------
DART_CSV_PATH = "DART기업목록.csv"   # (제공하신 경로 예시)
OUTPUT_DIR = "./krx_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_YEAR = 2014
END_YEAR = 2025  # 이 값은 스크립트가 현재일을 기준으로 잘라냅니다.
TODAY = date(2025, 9, 13)  # (명시) 이 스크립트는 '현재'를 2025-09-13로 가정하여 가장 최근 완결 분기까지 수집합니다.

# 네트워크 재시도 설정
MAX_RETRIES = 3
SLEEP_BETWEEN_CALLS = 0.6  # 서버 부하 완화를 위해 호출 사이에 약간의 대기

# ---------- 유틸 함수 ----------
def clean_code(raw):
    """종목코드 문자열 정리: 숫자만 추출해 6자리로 zero-fill."""
    if pd.isna(raw):
        return None
    s = str(raw)
    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    code = "".join(digits)
    return code.zfill(6)

def generate_quarter_ends(start_year=2014, end_year=2025, today=TODAY):
    """start_year ~ end_year 사이의 모든 분기말 날짜(YYYY-MM-DD) 리스트 반환,
       단 오늘(today)보다 이후인 날짜는 제외."""
    q_months = [3, 6, 9, 12]
    q_ends = []
    for y in range(start_year, end_year+1):
        for m in q_months:
            last_day = calendar.monthrange(y, m)[1]
            d = date(y, m, last_day)
            if d <= today:
                q_ends.append(d)
    q_ends.sort()
    return q_ends

def safe_call(func, *args, **kwargs):
    """간단 재시도 래퍼."""
    for attempt in range(1, MAX_RETRIES+1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[WARN] 호출 실패 (시도 {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
            else:
                raise

# ---------- 주요 로직 ----------
def load_dart_list(path=DART_CSV_PATH):
    df = pd.read_csv(path)
    # 가능한 경우 Unnamed:0 컬럼을 firm_id로 사용(원본에 따라 다름)
    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "firm_id"})
    else:
        df["firm_id"] = df.index + 1
    # 종목코드 컬럼명은 '종목코드'로 가정. 없다면 컬럼명 확인 필요.
    if "종목코드" not in df.columns:
        raise ValueError("DART CSV에 '종목코드' 컬럼이 없습니다. 파일을 확인해주세요.")
    df["stock_code_raw"] = df["종목코드"]
    df["stock_code"] = df["stock_code_raw"].apply(clean_code)
    missing = df["stock_code"].isnull().sum()
    if missing > 0:
        print(f"[WARN] 종목코드 정리 후 결측: {missing}건")
    return df[["firm_id", "stock_code", "stock_code_raw"]]

def fetch_quarterly_market_table(ticker_list, quarter_dates):
    """각 분기말 날짜마다 KRX에서 시가총액 테이블+종가 병합 후 ticker_list로 필터하여 반환."""
    all_rows = []
    missing_info = {}  # date -> missing tickers
    for qd in tqdm(quarter_dates, desc="Quarters"):
        qstr = qd.strftime("%Y%m%d")
        # 1) 시가총액(상장주식수 포함)
        df_cap = safe_call(stock.get_market_cap, qstr)  # 전체종목 시가총액 테이블
        # 2) 종가(OHLCV)
        df_price = safe_call(stock.get_market_ohlcv, qstr)  # 전체시장 OHLCV (index: 티커)
        # 표준화: 인덱스 형식 맞추기(문자열, 6자리)
        df_cap.index = df_cap.index.astype(str).str.zfill(6)
        df_price.index = df_price.index.astype(str).str.zfill(6)
        # 필요한 컬럼만 취함
        cols_cap_map = {
            "시가총액": "market_cap_krw",
            "상장주식수": "shares_outstanding",
            "거래량": "volume",
            "거래대금": "value_traded"
        }
        # 어떤 컬럼이 있는지 확인
        available_cap_cols = [c for c in cols_cap_map.keys() if c in df_cap.columns]
        df_cap_sub = df_cap[available_cap_cols].rename(columns={k:cols_cap_map[k] for k in available_cap_cols})
        # price의 '종가' 사용
        if "종가" in df_price.columns:
            df_price_sub = df_price[["종가"]].rename(columns={"종가": "close_price"})
        else:
            df_price_sub = pd.DataFrame(index=df_price.index)  # 빈프레임(이상상황)
        # 병합
        merged = df_cap_sub.join(df_price_sub, how="outer")
        merged["as_of_date"] = qstr
        merged["period_yyyyq"] = f"{qd.year}-Q{((qd.month-1)//3)+1}"
        # 필터: 요청된 티커들만
        subset = merged.loc[merged.index.isin(ticker_list)].copy()
        subset["ticker"] = subset.index
        all_rows.append(subset)
        # 누락체크
        missing_tickers = [t for t in ticker_list if t not in merged.index]
        missing_info[qstr] = missing_tickers
        time.sleep(SLEEP_BETWEEN_CALLS)
    if len(all_rows) == 0:
        return pd.DataFrame(), missing_info
    full = pd.concat(all_rows)
    # 컬럼 정리
    full = full.reset_index(drop=True)
    # 날짜/숫자 타입 정리
    full["market_cap_krw"] = pd.to_numeric(full.get("market_cap_krw", pd.Series()), errors="coerce")
    full["shares_outstanding"] = pd.to_numeric(full.get("shares_outstanding", pd.Series()), errors="coerce")
    full["close_price"] = pd.to_numeric(full.get("close_price", pd.Series()), errors="coerce")
    # (선택) firm_id는 이후 매핑
    return full, missing_info

# ---------- 파이프라인 예시 ----------
def main():
    # 1) DART 목록 로드 및 정리
    dart_df = load_dart_list(DART_CSV_PATH)
    print(f"총 기업 수(입력): {len(dart_df)} / 고유 종목코드: {dart_df['stock_code'].nunique()}")
    # 샘플 출력
    print("샘플 정리된 티커 (상위 20):")
    print(dart_df['stock_code'].head(20).tolist())

    tickers = sorted(dart_df['stock_code'].dropna().unique().tolist())

    # 2) 분기 날짜 목록 생성 (최신 완결 분기까지만)
    quarter_dates = generate_quarter_ends(START_YEAR, END_YEAR, TODAY)
    print(f"수집할 분기(개수): {len(quarter_dates)} (마지막: {quarter_dates[-1]})")

    # 3) 데이터 수집
    market_df, missing_info = fetch_quarterly_market_table(tickers, quarter_dates)

    # 4) firm_id 매핑
    mapping = dict(zip(dart_df['stock_code'], dart_df['firm_id']))
    market_df["firm_id"] = market_df["ticker"].map(mapping)

    # 5) 파일 저장
    market_out_path = os.path.join(OUTPUT_DIR, "market_table_quarterly.csv")
    market_df.to_csv(market_out_path, index=False, encoding="utf-8-sig")
    print(f"저장 완료: {market_out_path}")

    # 6) (선택) 회계정보(accounting_table.csv)가 있으면 병합 및 파생변수 생성 예시
    acct_path = "./accounting_table.csv"
    if os.path.exists(acct_path):
        acct = pd.read_csv(acct_path)
        # 기간키(예: '2025-Q2' 또는 '2025-Q2' 형식 맞추기 필요) - 여기서는 'period_yyyyq' 컬럼 기준으로 병합 예시
        merged = pd.merge(market_df, acct, how="left", left_on=["firm_id", "period_yyyyq"], right_on=["firm_id", "period"])
        # 파생지표 예시
        merged["Tobin_Q"] = (merged["market_cap_krw"] + merged["total_liabilities"]) / merged["total_assets"]
        merged["PBR_calc"] = merged["market_cap_krw"] / merged["equity"]
        panel_out = os.path.join(OUTPUT_DIR, "panel_agg_example.csv")
        merged.to_csv(panel_out, index=False, encoding="utf-8-sig")
        print(f"패널 병합본 저장: {panel_out}")
    else:
        print(f"[INFO] accounting_table.csv 파일이 없어 회계와의 병합은 생략했습니다. 병합하려면 같은 경로에 accounting_table.csv를 놓고 'period' 컬럼(예: 'YYYY-Qn')으로 정렬하세요.")

if __name__ == "__main__":
    main()
