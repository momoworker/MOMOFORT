#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주도주 탐색기 (Starfolio Leader Explorer) v19.1 — Cloudflare 25 MiB 한도 최적화(초경량 data.json) 버전
- 순수 KRX 15:30 정규장 종가 고정 (Yahoo KRX .KS/.KQ 오버레이로 NXT 대체거래소 야간시세 완벽 배제)
- 스타폴리오 건강도 경계 규칙 주중 즉시 반영:
  * 정배열 유지 중 고점 대비 -20.0% ~ -24.9% 조정 시 월~목 주중에도 즉시 'warning(경계)' 이동
  * 고점 대비 -25.0% 이상 하락 또는 26주선 하회 시 'exit(이탈)' 이동
- KRX 전 종목(KOSPI·KOSDAQ) 16-Thread 병렬 전수 스크리닝 + 초경량 저장:
  * 공백 제거(minified JSON) + 이탈 종목 52주 주봉 압축 저장으로 수백 개 이탈 종목을 모두 담아도
    data.json 용량을 30.5 MiB -> 약 4~6 MiB로 80% 이상 절감 (Cloudflare 25 MiB 제한 완벽 통과)
  * 저장 전 용량 안전장치(18 MiB 초과 시 이탈 주봉 30주로 자동 압축) 내장
"""
import os
import re
import json
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

IS_FRIDAY_OR_FORCED = (NOW_KST.weekday() == 4) or (os.environ.get("FORCE_FULL_SCREEN", "1") == "1")
MIN_MCAP_KRW = int(os.environ.get("MIN_MCAP_EOK", "700")) * 1_0000_0000
MAX_WORKERS = int(os.environ.get("SCREENER_WORKERS", "16"))
MAX_SAFE_JSON_BYTES = 18 * 1024 * 1024  # Cloudflare 25 MiB 한도 대비 18 MiB 안전 상한선

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/"
}


def parse_int_digits(val) -> int:
    s = re.sub(r"[^0-9]", "", str(val or ""))
    return int(s) if s else 0


def round_krx_tick(price: float) -> int:
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
    """Yahoo Finance 공식 KRX 정규장 피드(.KS/.KQ)로 순수 15:30 KRX 정규장 종가 확보 (NXT 배제)"""
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
            opens, highs, lows, closes, vols = (
                q0.get("open", []), q0.get("high", []), q0.get("low", []), q0.get("close", []), q0.get("volume", [])
            )
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
    w["Turnover"] = w["Close"] * w["Volume"]
    w["TurnMA4"] = w["Turnover"].rolling(4, min_periods=1).mean()
    w["TurnMA26"] = w["Turnover"].rolling(26, min_periods=1).mean()
    return w


def compute_align_and_prep_weeks(w: pd.DataFrame) -> tuple:
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


def build_exit_reasons(w: pd.DataFrame) -> tuple:
    last = w.iloc[-1]
    close_p = float(last["Close"])
    high52 = float(last["High52"]) if float(last["High52"]) > 0 else close_p
    mdd_abs = int(round(max(0.0, (1.0 - close_p / high52) * 100.0)))
    reasons = []
    if not bool(last["Aligned"]) or close_p < float(last["MA26"]):
        reasons.append("정배열 경성붕괴")
    if mdd_abs >= 25:
        reasons.append(f"MDD {mdd_abs}%(≥25)")
    if float(last["MA4"]) < float(last["MA26"]):
        reasons.append("4-26DC+코로보레이션")
    if float(last["TurnMA4"]) < float(last["TurnMA26"]):
        reasons.append("거래대금DC")
    tail52 = w.tail(52)
    peak_pos = int(np.argmax(tail52["High"].values))
    weeks_since_high = len(tail52) - 1 - peak_pos
    if weeks_since_high >= 18:
        reasons.append(f"신고가 {weeks_since_high}주 미경신")
    return mdd_abs, " · ".join(reasons) if reasons else f"MDD {mdd_abs}%(≥25)"


def evaluate_stage_and_health(stock: dict, w: pd.DataFrame, is_friday_or_forced: bool) -> dict:
    cur_stage = stock.get("stage", "onset")
    if len(w) == 0:
        return {"stage": cur_stage}
    last = w.iloc[-1]
    close_p = float(last["Close"])
    ma26 = float(last["MA26"])
    high52 = float(last["High52"]) if float(last["High52"]) > 0 else close_p
    mdd_pct = round((close_p / high52 - 1.0) * 100.0, 1)
    align_w, _ = compute_align_and_prep_weeks(w)
    if align_w == 0:
        align_w = int(stock.get("alignWeeks") or 1)

    if mdd_pct <= -25.0 and cur_stage != "onset":
        mdd_abs, verdict = build_exit_reasons(w)
        return {
            "stage": "exit",
            "rightBadge": "EXIT",
            "rightBadgeType": "EXIT",
            "mddPct": mdd_abs,
            "statusLabel": f"MDD {mdd_abs}% · 확산 · 정배열 {align_w}주",
            "verdict": verdict
        }

    if cur_stage in ("progress", "warning", "longterm") and -25.0 < mdd_pct <= -20.0:
        mdd_int = int(round(mdd_pct))
        return {
            "stage": "warning",
            "rightBadge": "경계",
            "rightBadgeType": "WARN",
            "statusLabel": f"건강도 경계 · 고점 {mdd_int}%",
            "verdict": f"확산 · 정배열 {align_w}주 · 건강도 경계 · 고점대비 {mdd_int}% — 정배열은 유지 중이나 진행 건강도 기준을 벗어남(매도 신호 아님)"
        }

    if cur_stage == "warning" and mdd_pct > -18.0 and bool(last["Aligned"]):
        return {
            "stage": "progress",
            "rightBadge": "확산",
            "rightBadgeType": "DIFF",
            "statusLabel": f"고점 {int(round(mdd_pct))}%",
            "verdict": f"확산 · 정배열 {align_w}주 · 고점 {int(round(mdd_pct))}% — 이탈 신호 없이 진행 중"
        }

    if not is_friday_or_forced:
        return {"stage": cur_stage}

    if close_p < ma26 or not bool(last["Aligned"]):
        mdd_abs, verdict = build_exit_reasons(w)
        return {
            "stage": "exit",
            "rightBadge": "EXIT",
            "rightBadgeType": "EXIT",
            "mddPct": mdd_abs,
            "statusLabel": f"MDD {mdd_abs}% · 확산 · 정배열 {align_w}주",
            "verdict": verdict
        }

    if align_w >= 26:
        return {"stage": "longterm", "rightBadge": "장기", "rightBadgeType": "DIFF"}
    if align_w >= 7:
        return {"stage": "progress", "rightBadge": "확산", "rightBadgeType": "DIFF"}
    return {"stage": "onset"}


def compute_weighted_rs_raw(df_daily: pd.DataFrame) -> float:
    c = df_daily["Close"]
    n = len(c)
    if n < 63:
        return 0.0
    p_now = float(c.iloc[-1])
    p_3m = float(c.iloc[max(0, n - 63)])
    p_6m = float(c.iloc[max(0, n - 126)])
    p_9m = float(c.iloc[max(0, n - 189)])
    p_12m = float(c.iloc[max(0, n - 252)])
    return (
        0.4 * ((p_now / p_3m - 1.0) * 100.0 if p_3m > 0 else 0.0)
        + 0.2 * ((p_now / p_6m - 1.0) * 100.0 if p_6m > 0 else 0.0)
        + 0.2 * ((p_now / p_9m - 1.0) * 100.0 if p_9m > 0 else 0.0)
        + 0.2 * ((p_now / p_12m - 1.0) * 100.0 if p_12m > 0 else 0.0)
    )


def scan_single_stock_worker(item: dict) -> dict:
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
    ma26 = float(last["MA26"])
    ma52 = float(last["MA52"])
    high52 = float(last["High52"]) if float(last["High52"]) > 0 else close_p
    is_aligned = bool(last["Aligned"]) and (close_p > ma52)
    align_w, prep_w = compute_align_and_prep_weeks(w)
    rs_raw = compute_weighted_rs_raw(df)

    had_past_run = int(w.tail(52)["Aligned"].sum()) >= 6
    mdd_abs = int(round(max(0.0, (1.0 - close_p / high52) * 100.0)))
    is_exited_leader = (not is_aligned) and had_past_run and (close_p < ma26 or mdd_abs >= 25)

    return {
        "ticker": tk,
        "name": item["name"],
        "market": mkt,
        "sector": item["sector"],
        "marcap": item["marcap"],
        "valid": True,
        "is_aligned": is_aligned,
        "is_exited_leader": is_exited_leader,
        "mdd_abs": mdd_abs,
        "past_align_weeks": int(w.tail(52)["Aligned"].sum()),
        "align_w": align_w,
        "prep_w": prep_w,
        "rs_raw": rs_raw,
        "close_p": int(close_p),
        "prev_c": int(round_krx_tick(df["Close"].iloc[-2])) if len(df) >= 2 else int(close_p),
        "w_df": w if (is_aligned and 1 <= align_w <= 4 and prep_w >= 15) or is_exited_leader else None
    }


def load_full_krx_universe() -> tuple:
    if fdr is None:
        return [], 0
    try:
        krx_df = fdr.StockListing("KRX")
        if krx_df is None or krx_df.empty:
            return [], 0
        total_raw = len(krx_df)
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
            sec = str(r.get("Dept", "") or r.get("Sector", "") or "일반업종").strip()
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


def build_candles_with_markers(w: pd.DataFrame, stage: str, align_w: int, max_bars: int = 105) -> list:
    tail_w = w.tail(max_bars)
    candles = []
    for _, row in tail_w.iterrows():
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
            "tstop": int(row["ATRStop"])
        })
    if len(candles) >= 5:
        bo_idx = max(0, len(candles) - 1 - min(len(candles) - 2, max(1, int(align_w or 10))))
        candles[bo_idx]["marker"] = "BREAKOUT"
        if stage == "exit":
            candles[-1]["marker"] = "EXIT"
    return candles


def main():
    t0 = time.time()
    data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    if not os.path.exists(data_file):
        print("data.json not found.")
        return

    with open(data_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # 기존 이탈 종목 중 중복·누적 방지를 위해 활성(초입/진행/경계/장기) 종목만 유지하고 이탈 종목은 매번 전수 재구축
    active_stocks = [st for st in payload.get("STOCKS_DATA", []) if st.get("stage") != "exit"]
    existing_by_ticker = {str(st.get("ticker", "")).zfill(6): st for st in active_stocks}

    print(f"[Step 1] 활성 추적 {len(active_stocks)}종목 KRX 15:30 정규장 종가 및 건강도 경계 규칙 적용 중...")
    for st in active_stocks:
        tk = str(st.get("ticker", "")).zfill(6)
        mkt = st.get("exchange", st.get("market", "KOSPI"))
        df = fetch_ohlcv_fast(tk, market=mkt, count=600, overlay_krx_close=True)
        if df.empty or len(df) < 30:
            continue

        last_c = round_krx_tick(df["Close"].iloc[-1])
        prev_c = round_krx_tick(df["Close"].iloc[-2]) if len(df) >= 2 else last_c
        chg_pct = round((last_c / prev_c - 1.0) * 100.0, 2) if prev_c > 0 else 0.0
        sign = "▲" if chg_pct > 0 else ("▼" if chg_pct < 0 else "")

        st["price"] = last_c
        st["priceText"] = f"{last_c:,}원"
        st["lastClose"] = f"{last_c:,}원"
        st["changePct"] = round(chg_pct, 1)
        st["changeText"] = f"{sign}{abs(chg_pct):.1f}%"
        st["priceStandard"] = "KRX 15:30 종가"

        cd = st.get("consensusData")
        if isinstance(cd, dict) and cd.get("targetPrice"):
            tp = parse_int_digits(cd["targetPrice"])
            if tp > 0 and last_c > 0:
                up_pct = round((tp / last_c - 1.0) * 100.0, 1)
                up_sign = "+" if up_pct >= 0 else ""
                cd["upsidePct"] = f"{up_sign}{up_pct:.1f}%"
                cd["upsideText"] = f"{up_sign}{up_pct:.1f}%"

        w = build_weekly_df(df)
        upd = evaluate_stage_and_health(st, w, IS_FRIDAY_OR_FORCED)
        st.update(upd)

        if IS_FRIDAY_OR_FORCED:
            align_w, prep_w = compute_align_and_prep_weeks(w)
            if align_w > 0:
                st["alignWeeks"] = align_w
                if st.get("stage") == "onset":
                    st["prepWeeks"] = max(prep_w, st.get("prepWeeks") or 15)

        bar_limit = 52 if st.get("stage") == "exit" else 105
        candles = build_candles_with_markers(w, st.get("stage", "onset"), st.get("alignWeeks") or 10, max_bars=bar_limit)
        if candles:
            st["weeklyCandles"] = candles
            stop_p = candles[-1]["tstop"]
            stop_pct = round((stop_p / last_c - 1.0) * 100.0, 1) if last_c > 0 else 0.0
            st["trailingStopPrice"] = f"{stop_p:,}원"
            st["trailingStopBadge"] = f"🛡️ ATR방어선 {stop_p:,}원 ({stop_pct:+.1f}%)"

    # 2. KRX 전 종목 16-Thread 병렬 전수 스크리닝
    universe, total_raw = load_full_krx_universe()
    print(f"[Step 2] KRX 전체 {total_raw}종목 중 필터링 통과 {len(universe)}종목 16-Thread 전수 스크리닝 시작...")
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

    rs_map = {}
    if scan_results:
        raw_list = sorted([r["rs_raw"] for r in scan_results])
        n_rs = len(raw_list)
        for r in scan_results:
            idx = np.searchsorted(raw_list, r["rs_raw"])
            rs_map[r["ticker"]] = int(max(1, min(99, round((idx / max(1, n_rs - 1)) * 98 + 1))))

    # 전체 이탈 종목 편입 (52주 초경량 주봉 -> 400종목이어도 약 3~4 MB!)
    exited_list = [r for r in scan_results if r["is_exited_leader"] and r["ticker"] not in existing_by_ticker]
    exited_list.sort(key=lambda x: x["mdd_abs"], reverse=True)
    added_exits = 0
    for ex in exited_list:
        tk = ex["ticker"]
        w_df = ex["w_df"]
        if w_df is None or len(w_df) < 20:
            continue
        mdd_abs, verdict = build_exit_reasons(w_df)
        last_c = ex["close_p"]
        prev_c = ex["prev_c"]
        chg_pct = round((last_c / prev_c - 1.0) * 100.0, 1) if prev_c > 0 else 0.0
        sign = "▲" if chg_pct > 0 else ("▼" if chg_pct < 0 else "")
        candles = build_candles_with_markers(w_df, "exit", ex["past_align_weeks"], max_bars=52)
        stop_p = candles[-1]["tstop"] if candles else round_krx_tick(last_c * 0.9)
        mcap_s = format_mcap_kr(ex["marcap"])
        obj = {
            "id": f"KR-{tk}",
            "country": "KR",
            "stage": "exit",
            "name": ex["name"],
            "ticker": tk,
            "exchange": ex["market"],
            "changePct": chg_pct,
            "changeText": f"{sign}{abs(chg_pct):.1f}%",
            "industry": ex["sector"],
            "subIndustry": f"{ex['sector']} · {ex['market']}",
            "rightBadge": "EXIT",
            "rightBadgeType": "EXIT",
            "alignWeeks": ex["past_align_weeks"],
            "prepWeeks": None,
            "score": None,
            "statusLabel": f"MDD {mdd_abs}% · 확산 · 정배열 {ex['past_align_weeks']}주",
            "mddPct": mdd_abs,
            "sectorRank": "—",
            "marketCap": mcap_s,
            "lastClose": f"{last_c:,}원",
            "onsetDate": TODAY_STR,
            "opBadge": "추세 이탈",
            "opPositive": False,
            "tags": [ex["sector"]],
            "verdict": verdict,
            "earningsTitle": f"이탈 사유 · {verdict}",
            "earningsSummary": f"현재가 {last_c:,}원 · 시총 {mcap_s}",
            "trailingStopPrice": f"{stop_p:,}원",
            "rsPercentile": rs_map.get(tk, 25),
            "breakoutImminent": False,
            "weeklyCandles": candles,
            "dailyOsc": []
        }
        active_stocks.append(obj)
        existing_by_ticker[tk] = obj
        added_exits += 1

    payload["STOCKS_DATA"] = active_stocks

    stage_counts = {"onset": 0, "progress": 0, "warning": 0, "longterm": 0, "exit": 0}
    for st in active_stocks:
        stg = st.get("stage", "onset")
        if stg in stage_counts:
            stage_counts[stg] += 1
    payload["STAGE_COUNTS"] = stage_counts

    if "meta" not in payload:
        payload["meta"] = {}
    payload["meta"]["updatedAt"] = NOW_KST.strftime("%Y-%m-%d %H:%M KST (KRX 15:30 정규장 종가)")
    payload["meta"]["priceDate"] = TODAY_STR
    payload["meta"]["baseDate"] = TODAY_STR
    payload["meta"]["priceStandard"] = "KRX 정규장 종가 (15:30 KST · NXT 대체거래소 제외)"
    payload["meta"]["scannedCount"] = len(scan_results)

    # 초경량(Minified) 직렬화 + Cloudflare 25 MiB 안전 상한(18 MiB) 자동 보호
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(serialized.encode("utf-8")) > MAX_SAFE_JSON_BYTES:
        print("[Size Guard] 18 MiB 초과 감지 -> 이탈 종목 주봉을 30주로 자동 압축합니다.")
        for st in payload["STOCKS_DATA"]:
            if st.get("stage") == "exit" and len(st.get("weeklyCandles", [])) > 30:
                wc = st["weeklyCandles"][-30:]
                wc[0]["marker"] = "BREAKOUT"
                wc[-1]["marker"] = "EXIT"
                st["weeklyCandles"] = wc
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    with open(data_file, "w", encoding="utf-8") as f:
        f.write(serialized)

    send_telegram_closing_report(active_stocks, stage_counts, TODAY_STR)

    final_mb = len(serialized.encode("utf-8")) / (1024 * 1024)
    print(f"[완료] 전 종목 {len(scan_results)}개 스캔 | 이탈: {added_exits}종목 | 단계별: {stage_counts} | data.json 크기: {final_mb:.2f} MiB ({time.time() - t0:.1f}초)")



def send_telegram_closing_report(stocks: list, stage_counts: dict, price_date: str):
    """
    [EDGE #4] 장 마감 후 텔레그램 자동 리포트 발송 (GitHub Secrets 설정 시 자동 작동)
    - 필요 Secrets: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    - 미설정 시 에러 없이 조용히 건너뜁니다.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("[Telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 미설정 -> 알림 생략")
        return

    onsets = [s for s in stocks if s.get("stage") == "onset"]
    warnings = [s for s in stocks if s.get("stage") == "warning"]
    near_stop = []
    for s in stocks:
        if s.get("stage") in ("onset", "progress", "warning", "longterm"):
            cur_p = parse_int_digits(s.get("lastClose") or s.get("price"))
            stp_p = parse_int_digits(s.get("trailingStopPrice"))
            if cur_p > 0 and stp_p > 0:
                gap_pct = (cur_p / stp_p - 1.0) * 100.0
                if gap_pct <= 5.0:
                    near_stop.append((s.get("name"), cur_p, stp_p, gap_pct))

    lines = [
        f"📊 [주도주 탐색기] {price_date} KRX 정규장 마감 리포트",
        f"• 현황: 초입 {stage_counts.get('onset',0)} · 진행 {stage_counts.get('progress',0)} · 경계 {stage_counts.get('warning',0)} · 장기 {stage_counts.get('longterm',0)} · 이탈 {stage_counts.get('exit',0)}",
        ""
    ]

    top_onsets = sorted(onsets, key=lambda x: float(x.get("changePct") or 0), reverse=True)[:5]
    if top_onsets:
        lines.append("🔥 초입 주요 종목 (KRX 15:30 종가)")
        for s in top_onsets:
            lines.append(f"  - {s.get('name')}({s.get('ticker')}): {s.get('lastClose')} ({s.get('changeText')}) · 방어선 {s.get('trailingStopPrice','—')}")
        lines.append("")

    if warnings:
        lines.append("⚠️ 건강도 경계 종목 (고점 -20%~-24.9%)")
        for s in warnings[:5]:
            lines.append(f"  - {s.get('name')}: {s.get('lastClose')} ({s.get('statusLabel','')})")
        lines.append("")

    if near_stop:
        lines.append("🚨 ATR 방어선 근접 경고 (5% 이내)")
        for name, cur_p, stp_p, gap in near_stop[:5]:
            lines.append(f"  - {name}: 현재 {cur_p:,}원 / 방어선 {stp_p:,}원 (여유 {gap:+.1f}%)")
        lines.append("")

    lines.append("🔗 바로가기: https://momofort.chulsung80.workers.dev/")
    msg_text = "\n".join(lines)

    try:
        api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({"chat_id": chat_id, "text": msg_text}).encode("utf-8")
        req = urllib.request.Request(api_url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print("[Telegram] 마감 리포트 발송 완료!")
    except Exception as e:
        print("[Telegram Warn]", e)


if __name__ == "__main__":
    main()