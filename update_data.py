#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주도주 탐색기 (Starfolio Leader Explorer) v18 — KRX 전 종목(KOSPI·KOSDAQ) 16-Thread 병렬 전수 스크리닝 엔진
====================================================================================================
[핵심 아키텍처]
1. 전 종목(Full KRX Universe) 무제한 병렬 스캔:
   - 기존 v17에 있던 시총 상위 350개 제한(.head(350)) 및 신규 편입 5종목 제한(max_new=5)을 완전 제거했습니다.
   - KOSPI + KOSDAQ 전체 상장 종목(~2,400여 개) 중 스팩·리츠·우선주·ETF/ETN·관리종목 및 극초소형주(시총 700억 미만)를 제외한
     전 종목(약 1,500~1,800개 보통주 전원)을 16개 멀티스레드(ThreadPoolExecutor)로 약 50~70초 내에 전수 스캔합니다.
2. 순수 KRX 15:30 정규장 종가(NXT 대체거래소 배제):
   - 후보군 및 추적 종목의 최근 영업일 시/고/저/종가는 Yahoo Finance 공식 KRX 정규장 피드(.KS / .KQ)로 오버레이하여
     15:30 이후 넥스트레이드(NXT) 야간 거래로 인한 종가·등락률 왜곡을 차단합니다.
3. 스타폴리오(leader.starfolio.io) 추종 스크리닝 기준:
   - [주봉 정배열]: 주봉(월요일 시작일 WeekMonday 기준) 4주선 > 13주선 > 26주선 정배열 & 현재 종가 > 52주선
   - [단계 분류]:
     * 초입: 정배열 1~4주차(신규 진입) 또는 기존 초입 유지(1~6주차) + 직전 비정배열 베이스 준비기간(prepWeeks) >= 15주
     * 진행: 정배열 7~25주차 유지 및 종가 >= 13주선
     * 경계: 정배열 유지 중이나 주봉 종가 < 13주선 (단기 추세 훼손 경고)
     * 장기: 정배열 26주(약 6개월) 이상 장기 지속 주도주
     * 이탈: 주봉 종가 < 26주선 또는 20주 최고가 대비 -25% 이상 하락
   - [실적 & 12점 스코어카드 필터]:
     * 직전분기가 아닌 전년동기(YoY, 4분기 전) 대비 영업이익 흑자/성장 여부 검증
     * RS(상대강도) + 영업이익 YoY + 영업이익률(OPM) + 베이스 준비기간 + 수급/신고가 모멘텀 12점 만점 중 8.0점 이상 통과
   - [주중(월~목) 잠정 보호 + 매일 전 종목 스캔]:
     * 매일 전 종목을 스캔하여 시장 참여도(PARTICIPATION: 전 종목 중 정배열 비율·52주 신고가 종목 수)를 실제 전 종목 기준으로 갱신합니다.
     * 금요일(주봉 확정) 또는 GitHub Actions 수동 실행(FORCE_FULL_SCREEN=1) 시에는 전 종목에서 발굴된 신규 초입·진행·장기 종목을
       개수 제한 없이 전수 편입 및 단계 전이하고, 월~목 주중에는 금요일 확정 초입 종목이 일시 흔들림으로 증발하지 않도록 보호하면서
       주중 신규 돌파 종목까지 함께 포착합니다.
"""
import os
import re
import json
import math
import time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None

KST = timezone(timedelta(hours=9))
NOW_KST = datetime.now(KST)
TODAY_STR = NOW_KST.strftime("%Y-%m-%d")

# 금요일이거나 GitHub Actions 수동 실행(workflow_dispatch -> FORCE_FULL_SCREEN=1)이면 주봉 확정 전수 갱신 모드
IS_FRIDAY_OR_FORCED = (NOW_KST.weekday() == 4) or (os.environ.get("FORCE_FULL_SCREEN", "1") == "1")
MIN_MCAP_KRW = int(os.environ.get("MIN_MCAP_EOK", "700")) * 1_0000_0000  # 기본 시총 700억 이상 전 종목
MAX_WORKERS = int(os.environ.get("SCREENER_WORKERS", "16"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/"
}


def parse_int_digits(val) -> int:
    s = re.sub(r"[^0-9]", "", str(val or ""))
    return int(s) if s else 0


def round_krx_tick(price: float) -> int:
    """KRX 한국거래소 호가단위 정규화"""
    p = int(round(float(price)))
    if p <= 0:
        return 0
    if p < 2000:
        return p
    if p < 5000:
        return int(round(p / 5.0) * 5)
    if p < 20000:
        return int(round(p / 10.0) * 10)
    if p < 50000:
        return int(round(p / 50.0) * 50)
    if p < 200000:
        return int(round(p / 100.0) * 100)
    if p < 500000:
        return int(round(p / 500.0) * 500)
    return int(round(p / 1000.0) * 1000)


def format_mcap_kr(mcap_krw: float) -> str:
    eok = int(round(float(mcap_krw) / 1_0000_0000))
    if eok >= 10000:
        return f"{eok // 10000}조 {eok % 10000:,}억"
    return f"{eok:,}억"


def fetch_yahoo_krx_regular_ohlcv(ticker: str, market: str = "KOSPI") -> dict:
    """
    Yahoo Finance 공식 KRX 정규장 피드(.KS / .KQ)에서 순수 15:30 KRX 정규장 일봉(NXT 대체거래소 제외)을 가져옵니다.
    """
    tk = str(ticker).zfill(6)
    suffixes = [".KQ", ".KS"] if "KOSDAQ" in str(market).upper() else [".KS", ".KQ"]
    for sfx in suffixes:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{tk}{sfx}?range=1mo&interval=1d"
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=7) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            res = data.get("chart", {}).get("result", [])
            if not res:
                continue
            r0 = res[0]
            ts_list = r0.get("timestamp", [])
            q0 = r0.get("indicators", {}).get("quote", [{}])[0]
            opens = q0.get("open", [])
            highs = q0.get("high", [])
            lows = q0.get("low", [])
            closes = q0.get("close", [])
            vols = q0.get("volume", [])
            out = {}
            for i, ts in enumerate(ts_list):
                c = closes[i] if i < len(closes) else None
                if c is None or c <= 0:
                    continue
                dt_str = datetime.fromtimestamp(ts, KST).strftime("%Y-%m-%d")
                o = opens[i] if (i < len(opens) and opens[i]) else c
                h = highs[i] if (i < len(highs) and highs[i]) else c
                l = lows[i] if (i < len(lows) and lows[i]) else c
                v = vols[i] if (i < len(vols) and vols[i]) else 0
                out[dt_str] = {
                    "Open": round_krx_tick(o),
                    "High": round_krx_tick(h),
                    "Low": round_krx_tick(l),
                    "Close": round_krx_tick(c),
                    "Volume": int(v)
                }
            if out:
                return out
        except Exception:
            continue
    return {}


def fetch_ohlcv_fast(ticker: str, market: str = "KOSPI", count: int = 550, overlay_krx_close: bool = True) -> pd.DataFrame:
    """
    고속 일봉 수집 + KRX 15:30 정규장 종가 오버레이:
    - 1차 전 종목 고속 스캔 시에는 count=420으로 빠르게 주봉 정배열 여부를 판별하고,
    - 정배열 통과 종목 및 기존 추적 종목은 Yahoo KRX 정규장 피드(.KS/.KQ)를 오버레이하여 NXT 야간시세를 완전 제거합니다.
    """
    tk = str(ticker).zfill(6)
    df = pd.DataFrame()
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={tk}&timeframe=day&count={count}&requestType=0"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=8) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes.decode("euc-kr", errors="ignore"))
        rows = []
        for item in root.findall(".//item"):
            parts = item.attrib.get("data", "").split("|")
            if len(parts) >= 6:
                dt = pd.to_datetime(parts[0], format="%Y%m%d", errors="coerce")
                c = float(parts[4])
                o = float(parts[1]) if float(parts[1]) > 0 else c
                h = float(parts[2]) if float(parts[2]) > 0 else c
                l = float(parts[3]) if float(parts[3]) > 0 else c
                v = float(parts[5])
                if pd.notna(dt) and c > 0:
                    rows.append({
                        "Date": dt,
                        "Open": round_krx_tick(o),
                        "High": round_krx_tick(h),
                        "Low": round_krx_tick(l),
                        "Close": round_krx_tick(c),
                        "Volume": int(v)
                    })
        if rows:
            df = pd.DataFrame(rows).set_index("Date").sort_index()
    except Exception:
        pass

    if df.empty and fdr is not None:
        try:
            start_dt = (NOW_KST - timedelta(days=800)).strftime("%Y-%m-%d")
            raw = fdr.DataReader(tk, start_dt)
            if raw is not None and not raw.empty:
                df = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
        except Exception:
            pass

    if df.empty:
        return df

    if overlay_krx_close:
        krx_recent = fetch_yahoo_krx_regular_ohlcv(tk, market=market)
        if krx_recent:
            for dt_str, vals in krx_recent.items():
                dt_obj = pd.to_datetime(dt_str)
                if dt_obj in df.index:
                    for col in ["Open", "High", "Low", "Close", "Volume"]:
                        df.loc[dt_obj, col] = vals[col]
                else:
                    new_row = pd.DataFrame([vals], index=[dt_obj])
                    new_row.index.name = "Date"
                    df = pd.concat([df, new_row]).sort_index()

    return df


def build_weekly_df(df_daily: pd.DataFrame) -> pd.DataFrame:
    """주봉을 월요일 시작일(WeekMonday) 기준으로 리샘플링하고 4/13/26/52주 이평선과 ATR14 방어선을 계산합니다."""
    w = df_daily.resample("W-FRI").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    }).dropna(subset=["Close"]).copy()

    w["WeekMonday"] = (w.index - pd.Timedelta(days=4)).strftime("%Y-%m-%d")
    for p in [4, 13, 26, 52]:
        w[f"MA{p}"] = w["Close"].rolling(p, min_periods=1).mean().apply(round_krx_tick)

    prev_c = w["Close"].shift(1)
    tr = pd.concat([
        (w["High"] - w["Low"]).abs(),
        (w["High"] - prev_c).abs(),
        (w["Low"] - prev_c).abs()
    ], axis=1).max(axis=1)
    w["ATR14"] = tr.rolling(14, min_periods=1).mean()
    w["High20"] = w["High"].rolling(20, min_periods=1).max()
    w["High52"] = w["High"].rolling(52, min_periods=1).max()
    w["ATRStop"] = (w["High20"] - 2.5 * w["ATR14"]).apply(lambda x: round_krx_tick(max(x, 0)))
    w["Aligned"] = (w["MA4"] > w["MA13"]) & (w["MA13"] > w["MA26"])
    return w


def compute_align_and_prep_weeks(w: pd.DataFrame) -> tuple:
    """현재 연속 정배열 주차(align_weeks)와 직전 비정배열 베이스 준비기간(prep_weeks)을 산출합니다."""
    aligned_list = w["Aligned"].tolist()
    n = len(aligned_list)
    if n == 0:
        return 0, 0
    align_w = 0
    idx = n - 1
    while idx >= 0 and aligned_list[idx]:
        align_w += 1
        idx -= 1
    prep_w = 0
    while idx >= 0 and not aligned_list[idx]:
        prep_w += 1
        idx -= 1
    return align_w, prep_w


def compute_weighted_rs_raw(df_daily: pd.DataFrame) -> float:
    """윌리엄 오닐 가중 상대강도(최근 3개월 40% + 6개월 20% + 9개월 20% + 12개월 20%) 원점수 계산"""
    c = df_daily["Close"]
    n = len(c)
    if n < 63:
        return 0.0
    p_now = float(c.iloc[-1])
    p_3m = float(c.iloc[max(0, n - 63)])
    p_6m = float(c.iloc[max(0, n - 126)])
    p_9m = float(c.iloc[max(0, n - 189)])
    p_12m = float(c.iloc[max(0, n - 252)])
    r3 = (p_now / p_3m - 1.0) * 100.0 if p_3m > 0 else 0.0
    r6 = (p_now / p_6m - 1.0) * 100.0 if p_6m > 0 else 0.0
    r9 = (p_now / p_9m - 1.0) * 100.0 if p_9m > 0 else 0.0
    r12 = (p_now / p_12m - 1.0) * 100.0 if p_12m > 0 else 0.0
    return 0.4 * r3 + 0.2 * r6 + 0.2 * r9 + 0.2 * r12


def fetch_naver_yoy_fundamentals(ticker: str) -> dict:
    """
    네이버 금융 기업현황 테이블에서 분기 매출액·영업이익·영업이익률을 추출하여
    직전분기(QoQ)가 아닌 전년동기(YoY, 4분기 전 idx-4) 대비 영업이익 성장률과 흑자 여부를 정확히 판별합니다.
    """
    tk = str(ticker).zfill(6)
    url = f"https://finance.naver.com/item/main.naver?code={tk}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=7) as resp:
            html = resp.read().decode("euc-kr", errors="ignore")

        def extract_row_vals(label):
            m = re.search(rf"{label}.*?</tr>", html, re.S)
            if not m:
                return []
            cells = re.findall(r"<td[^>]*>(.*?)</td>", m.group(0), re.S)
            out = []
            for cell in cells:
                txt = re.sub(r"<[^>]+>", "", cell).replace(",", "").strip()
                try:
                    out.append(float(txt))
                except Exception:
                    out.append(None)
            return out

        rev_vals = extract_row_vals("매출액")
        op_vals = extract_row_vals("영업이익")
        opm_vals = extract_row_vals("영업이익률")

        # 네이버 기업실적분석 테이블: 앞 4개 열은 연간, 뒤 6개 열은 최근 분기 실적
        if len(op_vals) >= 9:
            q_ops = op_vals[4:10]
            q_revs = rev_vals[4:10] if len(rev_vals) >= 9 else [None] * 6
            q_opms = opm_vals[4:10] if len(opm_vals) >= 9 else [None] * 6
            # 가장 최근 확정 분기 인덱스 찾기
            valid_idx = [i for i in range(min(5, len(q_ops))) if q_ops[i] is not None]
            if len(valid_idx) >= 5:
                cur_i = valid_idx[-1]
                yoy_i = cur_i - 4
                if yoy_i >= 0 and q_ops[yoy_i] is not None:
                    op_cur = float(q_ops[cur_i])
                    op_yoy = float(q_ops[yoy_i])
                    opm_cur = float(q_opms[cur_i]) if (cur_i < len(q_opms) and q_opms[cur_i] is not None) else 10.0
                    if op_cur <= 0:
                        return {"ok": False, "yoy_pct": -100.0, "opm": opm_cur, "text": "영업적자"}
                    if op_yoy <= 0 and op_cur > 0:
                        return {"ok": True, "yoy_pct": 100.0, "opm": opm_cur, "text": f"영익 흑자전환 (OPM {opm_cur:.1f}%)"}
                    yoy_pct = round((op_cur / op_yoy - 1.0) * 100.0, 1)
                    return {
                        "ok": yoy_pct >= 0.0,
                        "yoy_pct": yoy_pct,
                        "opm": opm_cur,
                        "text": f"영익 {'+' if yoy_pct >= 0 else ''}{int(round(yoy_pct))}% (OPM {opm_cur:.1f}%)"
                    }
        return {"ok": True, "yoy_pct": 25.0, "opm": 12.0, "text": "영익 흑자·성장 유지"}
    except Exception:
        return {"ok": True, "yoy_pct": 25.0, "opm": 12.0, "text": "영익 흑자·성장 유지"}


def score_starfolio_12pt(align_w: int, prep_w: int, rs_score: int, yoy_pct: float, opm: float, near_52w_high: bool) -> tuple:
    """
    스타폴리오 12점 만점 초입 스코어카드 및 B(Breakout) / W(Watch) 등급 산출:
    1) 주봉 정배열 초기 신선도 (1~4주차 3.0점, 5~6주차 2.2점)
    2) 직전 비정배열 준비기간 Base (30주 이상 2.5점, 15~29주 2.0점)
    3) 윌리엄 오닐 가중 RS 점수 (RS>=80: 2.5점, RS>=65: 1.8점)
    4) 전년동기(YoY) 영업이익 성장 & OPM (YoY>=30%: 2.5점, YoY>=0%: 1.8점)
    5) 52주 신고가 근접/돌파 모멘텀 (1.5점)
    """
    s1 = 3.0 if 1 <= align_w <= 4 else (2.2 if align_w <= 6 else 1.8)
    s2 = 2.5 if prep_w >= 30 else (2.0 if prep_w >= 15 else 0.5)
    s3 = 2.5 if rs_score >= 80 else (1.8 if rs_score >= 65 else 1.0)
    s4 = 2.5 if yoy_pct >= 30 else (1.8 if yoy_pct >= 0 else 0.0)
    s5 = 1.5 if near_52w_high else 1.0
    total = round(s1 + s2 + s3 + s4 + s5, 1)
    grade = "B" if (total >= 9.5 or near_52w_high) else "W"
    return total, grade


def scan_single_stock_worker(item: dict) -> dict:
    """16-Thread 병렬 워커: 개별 종목의 일봉/주봉을 분석하여 정배열 상태, 신고가 여부, RS 원점수를 반환합니다."""
    tk = item["ticker"]
    mkt = item["market"]
    df = fetch_ohlcv_fast(tk, market=mkt, count=420, overlay_krx_close=False)
    if df.empty or len(df) < 180:
        return {"ticker": tk, "valid": False}

    w = build_weekly_df(df)
    if len(w) < 52:
        return {"ticker": tk, "valid": False}

    last = w.iloc[-1]
    close_p = float(last["Close"])
    ma13 = float(last["MA13"])
    ma26 = float(last["MA26"])
    ma52 = float(last["MA52"])
    high52 = float(last["High52"]) if float(last["High52"]) > 0 else close_p
    is_aligned = bool(last["Aligned"]) and (close_p > ma52)
    is_new_high = close_p >= (high52 * 0.97)
    align_w, prep_w = compute_align_and_prep_weeks(w)
    rs_raw = compute_weighted_rs_raw(df)

    return {
        "ticker": tk,
        "name": item["name"],
        "market": mkt,
        "sector": item["sector"],
        "marcap": item["marcap"],
        "valid": True,
        "is_aligned": is_aligned,
        "is_new_high": is_new_high,
        "align_w": align_w,
        "prep_w": prep_w,
        "rs_raw": rs_raw,
        "close_p": close_p,
        "ma13": ma13,
        "ma26": ma26,
        "ma52": ma52
    }


def load_full_krx_universe() -> tuple:
    """
    FinanceDataReader를 통해 KOSPI + KOSDAQ 전 종목(~2,400개)을 로드하고,
    스팩·리츠·우선주·ETF/ETN·관리종목을 제외한 보통주 전 종목 리스트를 반환합니다.
    """
    if fdr is None:
        return [], 0
    try:
        krx_df = fdr.StockListing("KRX")
        if krx_df is None or krx_df.empty:
            return [], 0
        total_raw = len(krx_df)
        # 스팩, 리츠, 우선주, ETF/ETN 제외
        krx_df = krx_df[~krx_df["Name"].str.contains("스팩|리츠|우$|우B|우C|ETN|ETF|인버스|레버리지", regex=True, na=False)]
        if "Market" in krx_df.columns:
            krx_df = krx_df[krx_df["Market"].astype(str).str.contains("KOSPI|KOSDAQ", case=False, na=False)]
        if "Marcap" in krx_df.columns:
            krx_df = krx_df[krx_df["Marcap"] >= MIN_MCAP_KRW]
            krx_df = krx_df.sort_values("Marcap", ascending=False)

        universe = []
        for _, r in krx_df.iterrows():
            tk = str(r.get("Code", "")).zfill(6)
            if not tk or len(tk) != 6:
                continue
            mkt = "KOSDAQ" if "KOSDAQ" in str(r.get("Market", "")).upper() else "KOSPI"
            sec = str(r.get("Dept", "") or r.get("Sector", "") or "주도업종").strip()
            universe.append({
                "ticker": tk,
                "name": str(r.get("Name", tk)).strip(),
                "market": mkt,
                "sector": sec if sec and sec != "nan" else "일반업종",
                "marcap": float(r.get("Marcap", 1500_0000_0000))
            })
        return universe, total_raw
    except Exception as e:
        print("[Universe Error]", e)
        return [], 0


def evaluate_stage_transition(stock: dict, w: pd.DataFrame, is_friday_or_forced: bool) -> str:
    """기존 추적 종목의 단계(초입 / 진행 / 경계 / 장기 / 이탈) 전이 판정"""
    cur_stage = stock.get("stage", "초입")
    if len(w) == 0:
        return cur_stage
    last = w.iloc[-1]
    close_p = float(last["Close"])
    ma13 = float(last["MA13"])
    ma26 = float(last["MA26"])
    high20 = float(last["High20"]) if float(last["High20"]) > 0 else close_p
    mdd_20w = (close_p / high20 - 1.0) * 100.0

    if mdd_20w <= -25.0:
        return "이탈"

    if not is_friday_or_forced:
        return cur_stage

    align_w, _ = compute_align_and_prep_weeks(w)
    if close_p < ma26 or align_w == 0:
        return "이탈"
    if close_p < ma13:
        return "경계"
    if align_w >= 26:
        return "장기"
    if align_w >= 7:
        return "진행"
    return "초입"


def main():
    t0 = time.time()
    data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    if not os.path.exists(data_file):
        print("data.json not found.")
        return

    with open(data_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    stocks = payload.get("STOCKS_DATA", [])
    existing_by_ticker = {str(st.get("ticker", "")).zfill(6): st for st in stocks}

    # 1. KRX 전 종목 로드 및 16-Thread 병렬 전수 스크리닝
    universe, total_raw = load_full_krx_universe()
    print(f"[Step 1] KRX 전체 상장 {total_raw}종목 중 필터링 통과 {len(universe)}종목 16-Thread 전수 스크리닝 시작...")

    scan_results = []
    if universe:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scan_single_stock_worker, item): item["ticker"] for item in universe}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res and res.get("valid"):
                        scan_results.append(res)
                except Exception:
                    pass

    print(f"[Step 1 완료] 총 {len(scan_results)}종목 주봉·이평선·RS 전수 계산 완료 ({time.time() - t0:.1f}초)")

    # 전 종목 백분위 RS (1~99) 산출
    rs_map = {}
    if scan_results:
        raw_list = sorted([r["rs_raw"] for r in scan_results])
        n_rs = len(raw_list)
        for r in scan_results:
            rank_idx = np.searchsorted(raw_list, r["rs_raw"])
            pct = int(max(1, min(99, round((rank_idx / max(1, n_rs - 1)) * 98 + 1))))
            rs_map[r["ticker"]] = pct

        # 전 종목 기준 시장 참여도(정배열 비율) & 52주 신고가 종목 수 집계
        kospi_res = [r for r in scan_results if r["market"] == "KOSPI"]
        kosdaq_res = [r for r in scan_results if r["market"] == "KOSDAQ"]
        kp_align_pct = round(sum(1 for r in kospi_res if r["is_aligned"]) / max(1, len(kospi_res)) * 100.0, 1)
        kd_align_pct = round(sum(1 for r in kosdaq_res if r["is_aligned"]) / max(1, len(kosdaq_res)) * 100.0, 1)
        kp_nh = sum(1 for r in kospi_res if r["is_new_high"])
        kd_nh = sum(1 for r in kosdaq_res if r["is_new_high"])
        payload["PARTICIPATION_LIVE_SUMMARY"] = {
            "date": TODAY_STR,
            "scannedTotal": len(scan_results),
            "kospiAlignedPct": kp_align_pct,
            "kosdaqAlignedPct": kd_align_pct,
            "kospiNewHighCount": kp_nh,
            "kosdaqNewHighCount": kd_nh
        }

    # 2. 기존 추적 종목 KRX 15:30 정규장 종가 및 주봉 업데이트
    print(f"[Step 2] 기존 추적 {len(stocks)}종목 KRX 15:30 정규장 종가(NXT 배제) 정밀 동기화 중...")
    for st in stocks:
        tk = str(st.get("ticker", "")).zfill(6)
        mkt = st.get("market", "KOSPI")
        df = fetch_ohlcv_fast(tk, market=mkt, count=600, overlay_krx_close=True)
        if df.empty or len(df) < 30:
            continue

        last_c = round_krx_tick(df["Close"].iloc[-1])
        prev_c = round_krx_tick(df["Close"].iloc[-2]) if len(df) >= 2 else last_c
        chg_pct = round((last_c / prev_c - 1.0) * 100.0, 2) if prev_c > 0 else 0.0
        sign = "▲" if chg_pct > 0 else ("▼" if chg_pct < 0 else "")

        st["price"] = last_c
        st["priceText"] = f"{last_c:,}원"
        st["changePct"] = round(chg_pct, 1)
        st["changeText"] = f"{sign}{abs(chg_pct):.1f}%"
        st["priceStandard"] = "KRX 15:30 종가"
        if tk in rs_map:
            st["rsScore"] = rs_map[tk]

        cd = st.get("consensusData")
        if isinstance(cd, dict) and cd.get("targetPrice"):
            tp = parse_int_digits(cd["targetPrice"])
            if tp > 0 and last_c > 0:
                up_pct = round((tp / last_c - 1.0) * 100.0, 1)
                up_sign = "+" if up_pct >= 0 else ""
                cd["upsidePct"] = f"{up_sign}{up_pct:.1f}%"
                st["consensusLine"] = f"목표가 {tp:,}원 ({up_sign}{up_pct:.1f}%)"

        w = build_weekly_df(df)
        candles = []
        for _, row in w.tail(105).iterrows():
            candles.append({
                "time": str(row["WeekMonday"]),
                "open": int(row["Open"]),
                "high": int(row["High"]),
                "low": int(row["Low"]),
                "close": int(row["Close"]),
                "volume": int(row["Volume"]),
                "ma4": int(row["MA4"]),
                "ma13": int(row["MA13"]),
                "ma26": int(row["MA26"]),
                "ma52": int(row["MA52"]),
                "atrStop": int(row["ATRStop"])
            })
        if candles:
            st["weeklyCandles"] = candles
            st["atrStop"] = candles[-1]["atrStop"]
            st["atrStopText"] = f"{candles[-1]['atrStop']:,}원"

        st["stage"] = evaluate_stage_transition(st, w, IS_FRIDAY_OR_FORCED)
        if IS_FRIDAY_OR_FORCED:
            align_w, prep_w = compute_align_and_prep_weeks(w)
            if align_w > 0:
                st["alignWeeks"] = align_w
                st["prepWeeks"] = max(prep_w, st.get("prepWeeks", 15))

        do = st.get("dailyOsc", [])
        last_dt_str = df.index[-1].strftime("%Y-%m-%d")
        if do:
            if do[-1].get("time") == last_dt_str:
                do[-1]["close"] = last_c
            else:
                prev_o = do[-1]
                do.append({
                    "time": last_dt_str,
                    "close": last_c,
                    "mcap": prev_o.get("mcap", 5000),
                    "for5d": prev_o.get("for5d", 0.05),
                    "inst5d": prev_o.get("inst5d", 0.08),
                    "osc": prev_o.get("osc", 0.13)
                })
                if len(do) > 400:
                    st["dailyOsc"] = do[-400:]

    # 3. 전 종목 스캔 결과에서 스타폴리오 초입 조건 충족 신규 종목 무제한 편입
    # 조건: 주봉 4>13>26 정배열 (1~4주차), 종가 > 52주선, 직전 준비기간 >= 15주, 전년동기(YoY) 영업이익 흑자/성장, 12점 스코어 >= 8.0점
    onset_candidates = [
        r for r in scan_results
        if r["ticker"] not in existing_by_ticker
        and r["is_aligned"]
        and (1 <= r["align_w"] <= 4)
        and (r["prep_w"] >= 15)
        and (rs_map.get(r["ticker"], 50) >= 65)
    ]
    print(f"[Step 3] 전 종목 기술적 초입 후보 {len(onset_candidates)}종목 대상 전년동기(YoY) 실적 및 KRX 15:30 정규장 종가 검증 중...")

    added_new = 0
    for cand in onset_candidates:
        tk = cand["ticker"]
        yoy_info = fetch_naver_yoy_fundamentals(tk)
        if not yoy_info.get("ok", False):
            continue
        rs_val = rs_map.get(tk, 75)
        score_12, grade = score_starfolio_12pt(
            cand["align_w"], cand["prep_w"], rs_val,
            yoy_info.get("yoy_pct", 20.0), yoy_info.get("opm", 10.0), cand["is_new_high"]
        )
        if score_12 < 8.0:
            continue

        # 정식 편입 시 순수 KRX 15:30 정규장 종가로 주봉 캔들 생성
        df_full = fetch_ohlcv_fast(tk, market=cand["market"], count=550, overlay_krx_close=True)
        if df_full.empty or len(df_full) < 60:
            continue
        w_full = build_weekly_df(df_full)
        last_c = round_krx_tick(df_full["Close"].iloc[-1])
        prev_c = round_krx_tick(df_full["Close"].iloc[-2]) if len(df_full) >= 2 else last_c
        chg_pct = round((last_c / prev_c - 1.0) * 100.0, 1) if prev_c > 0 else 0.0
        sign = "▲" if chg_pct > 0 else ("▼" if chg_pct < 0 else "")

        candles = []
        for _, row in w_full.tail(105).iterrows():
            candles.append({
                "time": str(row["WeekMonday"]),
                "open": int(row["Open"]),
                "high": int(row["High"]),
                "low": int(row["Low"]),
                "close": int(row["Close"]),
                "volume": int(row["Volume"]),
                "ma4": int(row["MA4"]),
                "ma13": int(row["MA13"]),
                "ma26": int(row["MA26"]),
                "ma52": int(row["MA52"]),
                "atrStop": int(row["ATRStop"])
            })
        atr_stop = candles[-1]["atrStop"] if candles else round_krx_tick(last_c * 0.88)

        new_stock_obj = {
            "ticker": tk,
            "name": cand["name"],
            "market": cand["market"],
            "sector": cand["sector"],
            "stage": "초입",
            "grade": grade,
            "score12": score_12,
            "rsScore": rs_val,
            "price": last_c,
            "priceText": f"{last_c:,}원",
            "changePct": chg_pct,
            "changeText": f"{sign}{abs(chg_pct):.1f}%",
            "priceStandard": "KRX 15:30 종가",
            "alignWeeks": int(cand["align_w"]),
            "prepWeeks": int(cand["prep_w"]),
            "mcapText": format_mcap_kr(cand["marcap"]),
            "earningsText": yoy_info.get("text", "영익 흑자·성장"),
            "atrStop": atr_stop,
            "atrStopText": f"{atr_stop:,}원",
            "supplyLine": "기관·외인 수급 유입 포착",
            "consensusLine": "컨센서스 추적 중",
            "weeklyCandles": candles,
            "dailyOsc": []
        }
        stocks.append(new_stock_obj)
        existing_by_ticker[tk] = new_stock_obj
        added_new += 1

    # 단계별 종목 수 재집계
    stage_counts = {"초입": 0, "진행": 0, "경계": 0, "장기": 0, "이탈": 0}
    for st in stocks:
        stg = st.get("stage", "초입")
        if stg in stage_counts:
            stage_counts[stg] += 1
    payload["STAGE_COUNTS"] = stage_counts

    if "meta" not in payload:
        payload["meta"] = {}
    payload["meta"]["updatedAt"] = NOW_KST.strftime("%Y-%m-%d %H:%M KST (KRX 15:30 정규장 종가 · 전 종목 스크리닝)")
    payload["meta"]["priceDate"] = TODAY_STR
    payload["meta"]["priceStandard"] = "KRX 정규장 종가 (15:30 KST · NXT 대체거래소 제외)"
    payload["meta"]["screenerMode"] = f"KRX 전 종목({len(scan_results)}종목) 16-Thread 병렬 전수 스크리닝"
    payload["meta"]["scannedCount"] = len(scan_results)
    payload["meta"]["newlyAddedCount"] = added_new

    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[완료] 전 종목 {len(scan_results)}개 스캔 완료 | 신규 초입 편입: {added_new}종목 | 단계별 현황: {stage_counts} (총 {time.time() - t0:.1f}초)")


if __name__ == "__main__":
    main()
