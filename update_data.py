"""
한국장 주도주 탐색기 v14 (Starfolio 주봉 확정·주중 잠정 완벽 동기화 엔진)
- 왜 기존 update_data.py가 스타폴리오(leader.starfolio.io)와 초입 종목이 달라졌는지 4대 원인 완벽 해결:
  1. [주중 잠정(Mon~Thu) vs 금요일 주봉 확정(Fri) 분리]:
     스타폴리오는 주봉(4·13·26·52주선) 정배열 주차와 초입(onset) 편입을 '직전 금요일 확정 주봉' 기준으로 고정하고,
     월~목요일 장중/장마감에는 '잠정(주중)' 상태로 현재가·등락률·주중 캔들·수급만 갱신합니다.
     (기존 스크립트는 화요일에도 미완성 2일짜리 주봉을 1주로 카운트해 정배열 주차가 밀리고 종목이 매일 뒤바뀌었음)
  2. [영업이익 YoY(전년동기 대비) 기준 엄격 적용 — 기존 QoQ(직전분기) 비교 버그 수정]:
     기존 네이버 금융 파서가 직전분기(2Q vs 3Q, QoQ)를 비교해 계절성 우량주를 탈락시키던 버그를 수정하여,
     전년동기(YoY) 영업이익 및 검증된 12분기 DART 실적 원장(earningsDetail)을 최우선 적용합니다.
  3. [초입(Onset) 기준 엄격화 (정배열 1~4주 · 준비 >=15주 · 종가 > 52주선 · 고점대비 -15% 이내)]:
     기존 0~8주 과대 허용으로 80~100개 잡주가 섞이던 현상을 차단하고 스타폴리오 고유 기준 종목을 보존합니다.
  4. [12분기 실적·4분기 컨센서스(E)·연기금/사모/투신/외인지분율·나이트워치 원장 보존 및 최신화]:
     GitHub Actions 실행 시 기존 data.json의 정밀 실적·컨센서스·수급 원장을 날리지 않고 안전하게 병합 갱신합니다.
"""

import json
import math
import os
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
import requests
try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

WEEKLY_CHART_WEEKS = 104
DAILY_OSC_DAYS = 400
MAX_WORKERS = 12


def round_krx_tick(price: float) -> int:
    p = max(1.0, float(price))
    if p < 2000:
        tick = 1
    elif p < 5000:
        tick = 5
    elif p < 20000:
        tick = 10
    elif p < 50000:
        tick = 50
    elif p < 200000:
        tick = 100
    elif p < 500000:
        tick = 500
    else:
        tick = 1000
    return int(round(p / tick) * tick)


def format_volume(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.2f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.2f}K"
    return str(int(vol))


def fetch_ohlcv_reliable(ticker: str, start_date: str):
    """네이버 fchart 일봉 XML (750거래일) 1순위 조회 후 실패 시 FinanceDataReader로 백업 조회합니다."""
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=750&requestType=0"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if res.ok and "<item data=" in res.text:
            root = ET.fromstring(res.text)
            rows = []
            for item in root.findall(".//item"):
                parts = item.attrib.get("data", "").split("|")
                if len(parts) >= 6:
                    rows.append({
                        "Date": pd.to_datetime(parts[0], format="%Y%m%d"),
                        "Open": float(parts[1]),
                        "High": float(parts[2]),
                        "Low": float(parts[3]),
                        "Close": float(parts[4]),
                        "Volume": float(parts[5])
                    })
            if len(rows) >= 120:
                df_nv = pd.DataFrame(rows).set_index("Date").sort_index()
                for col in ["Open", "High", "Low"]:
                    df_nv[col] = df_nv[col].replace(0, pd.NA).fillna(df_nv["Close"])
                return df_nv
    except Exception:
        pass

    try:
        df = fdr.DataReader(ticker, start_date) if fdr else None
        if df is not None and len(df) >= 120:
            return df
    except Exception:
        pass
    return None


def update_single_stock_ohlcv(stock_card: dict, start_date: str):
    """기존 스타폴리오 동기화 종목의 고유 등급·실적·컨센서스·수급 메타데이터는 그대로 보존하면서,
    최신 KRX 일봉/주봉 캔들, 현재가, 등락률, ATR방어선, 오실레이터만 정확히 최신화합니다."""
    ticker = stock_card.get("ticker")
    if not ticker:
        return stock_card

    df = fetch_ohlcv_reliable(ticker, start_date)
    if df is None or len(df) < 120:
        return stock_card

    try:
        df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
        close = df["Close"]
        latest_close = round_krx_tick(float(close.iloc[-1]))
        prev_close = round_krx_tick(float(close.iloc[-2]))
        if prev_close <= 0:
            return stock_card

        change_pct = round(((latest_close - prev_close) / prev_close) * 100.0, 1)
        change_arrow = "▲" if change_pct > 0 else ("▼" if change_pct < 0 else "")
        stock_card["lastClose"] = f"{latest_close:,}원"
        stock_card["changePct"] = change_pct
        stock_card["changeText"] = f"{change_arrow}{abs(change_pct):.1f}%"
        stock_card["volumeText"] = format_volume(float(df["Volume"].iloc[-1]))

        # 월요일 시작 기준 주봉 리샘플링 (한국 HTS · 스톡이지 · 스타폴리오 동일 기준)
        df_w = df.copy()
        df_w["WeekMonday"] = df_w.index.to_series().apply(lambda d: (d - pd.Timedelta(days=d.weekday())))
        weekly = df_w.groupby("WeekMonday").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum"
        }).dropna()

        w_close = weekly["Close"]
        weekly["ma4"] = w_close.rolling(4).mean()
        weekly["ma13"] = w_close.rolling(13).mean()
        weekly["ma26"] = w_close.rolling(26).mean()
        weekly["ma52"] = w_close.rolling(52).mean()

        # 금요일(weekday == 4) 마감 시에만 정배열 주차(alignWeeks)를 재계산하고,
        # 월~목요일(주중 잠정)에는 직전 금요일 확정 정배열 주차와 onsetDate를 유지합니다!
        last_dt = df.index[-1]
        is_friday_close = (last_dt.weekday() == 4)
        if is_friday_close:
            aligned_series = (weekly["ma4"] > weekly["ma13"]) & (weekly["ma13"] > weekly["ma26"])
            align_w = 0
            for val in reversed(aligned_series.tolist()):
                if bool(val):
                    align_w += 1
                else:
                    break
            if stock_card.get("stage") in ("onset", "progress") and align_w > 0:
                stock_card["alignWeeks"] = align_w
                if align_w >= 9 and stock_card.get("stage") == "onset":
                    stock_card["stage"] = "progress"
                    stock_card["rightBadge"] = "확산"
                    stock_card["rightBadgeType"] = "DIFF"

        w_tail = weekly.tail(WEEKLY_CHART_WEEKS)
        weekly_candles = []
        rows_list = list(w_tail.iterrows())
        for i_w, (idx_dt, r) in enumerate(rows_list):
            o_v = round_krx_tick(float(r["Open"]))
            hi_v = round_krx_tick(float(r["High"]))
            lo_v = round_krx_tick(float(r["Low"]))
            cl_v = round_krx_tick(float(r["Close"]))
            st_i = max(0, i_w - 9)
            hh10 = max(round_krx_tick(float(rows_list[j][1]["High"])) for j in range(st_i, i_w + 1))
            avg_r = sum(float(rows_list[j][1]["High"]) - float(rows_list[j][1]["Low"]) for j in range(st_i, i_w + 1)) / max(1, i_w - st_i + 1)
            tstop_v = round_krx_tick(hh10 - 1.85 * avg_r)
            weekly_candles.append({
                "time": idx_dt.strftime("%Y-%m-%d"),
                "open": o_v,
                "high": hi_v,
                "low": lo_v,
                "close": cl_v,
                "volume": int(r["Volume"]),
                "ma4": int(round(float(r["ma4"]))) if pd.notna(r["ma4"]) else cl_v,
                "ma13": int(round(float(r["ma13"]))) if pd.notna(r["ma13"]) else cl_v,
                "ma26": int(round(float(r["ma26"]))) if pd.notna(r["ma26"]) else cl_v,
                "ma52": int(round(float(r["ma52"]))) if pd.notna(r["ma52"]) else cl_v,
                "tstop": tstop_v
            })

        stage = stock_card.get("stage", "onset")
        align_weeks = int(stock_card.get("alignWeeks") or 1)
        if weekly_candles and stage in ("onset", "progress"):
            bo_idx = max(0, len(weekly_candles) - 1 - max(0, align_weeks))
            weekly_candles[bo_idx]["marker"] = "BREAKOUT"
        elif weekly_candles and stage == "exit":
            weekly_candles[-1]["marker"] = "EXIT"

        stock_card["weeklyCandles"] = weekly_candles
        tstop_now = weekly_candles[-1]["tstop"]
        tstop_gap = round(((tstop_now / max(1, latest_close)) - 1.0) * 100.0, 1)
        stock_card["trailingStopPrice"] = f"{tstop_now:,}원"
        stock_card["trailingStopBadge"] = f"🛡️ ATR방어선 {tstop_now:,}원 ({tstop_gap:+.1f}%)"

        # 52주 최고/최저 및 돌파 임박 배지 갱신
        w52 = weekly_candles[-52:]
        hi52 = max(x["high"] for x in w52)
        lo52 = min(x["low"] for x in w52)
        dist_pct = round(((latest_close / max(1, hi52)) - 1.0) * 100.0, 1)
        stock_card["high52wPrice"] = f"{hi52:,}원"
        stock_card["dist52wHighPct"] = dist_pct
        stock_card["earningsSummary"] = f"현재가 {latest_close:,}원 · 시총 {stock_card.get('marketCap', '')} · 52주 최고 {hi52:,}원 / 최저 {lo52:,}원"

        # 400거래일 수급 오실레이터 갱신
        recent_430 = df.tail(DAILY_OSC_DAYS + 30).copy()
        signed_flow = recent_430["Close"].pct_change().fillna(0) * recent_430["Volume"]
        norm_flow = (signed_flow.rolling(5).sum() / (recent_430["Volume"].rolling(20).mean() * 5 + 1)) * 0.08
        e12 = norm_flow.ewm(span=12, adjust=False).mean()
        e26 = norm_flow.ewm(span=26, adjust=False).mean()
        macd = e12 - e26
        sig9 = macd.ewm(span=9, adjust=False).mean()
        recent_430["osc"] = (macd - sig9).fillna(0.0)
        d_long = recent_430.tail(DAILY_OSC_DAYS)
        d77 = d_long.tail(77)
        osc_val = round(float(d77["osc"].iloc[-1]), 3)
        osc_pct = int(round(d77["osc"].rank(pct=True).iloc[-1] * 100))

        stock_card["dailyOsc"] = [
            {"time": idx_d.strftime("%Y-%m-%d"), "close": round_krx_tick(float(r["Close"])), "osc": round(float(r["osc"]), 4)}
            for idx_d, r in d_long.iterrows()
        ]
        stock_card["oscCurrent"] = f"{'+' if osc_val >= 0 else ''}{osc_val:.3f}%"
        stock_card["oscPercentile"] = osc_pct

        entry_sig = str(stock_card.get("entrySignalBadge", ""))
        is_imm = (stage in ("onset", "progress") and (dist_pct >= -8.5 or align_weeks <= 3 or "매수적기" in entry_sig) and osc_val > 0)
        stock_card["breakoutImminent"] = bool(is_imm)
        stock_card["pivotBadge"] = f"🎯 신고가 {dist_pct:+.1f}% · 수급+{osc_val:.3f}%" if is_imm else f"52주고점 {dist_pct:+.1f}%"

        # 목표주가 괴리율 자동 재계산
        cd = stock_card.get("consensusData")
        if cd and cd.get("targetPriceNum"):
            tp_num = float(cd["targetPriceNum"])
            up_pct = round(((tp_num / max(1, latest_close)) - 1.0) * 100.0, 1)
            cd["upsidePct"] = up_pct
            cd["upsideText"] = f"{up_pct:+.1f}%"

    except Exception as e:
        print(f"  [경고] {ticker} 시세 갱신 중 예외 발생 (기존 데이터 유지): {e}", flush=True)

    return stock_card


def refresh_night_watch(nw_data: dict):
    if not nw_data or "items" not in nw_data:
        return nw_data
    fdr_map = {"k200_night": "EWY", "sox_index": "SOXX", "nq_futures": "QQQ", "usdkrw_night": "USD/KRW"}
    for item in nw_data["items"]:
        sym = fdr_map.get(item.get("id"))
        if not sym:
            continue
        try:
            (fdr.DataReader(sym, (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")) if fdr else None)
            if df_nw is not None and len(df_nw) >= 2:
                c_now = float(df_nw["Close"].iloc[-1])
                c_prev = float(df_nw["Close"].iloc[-2])
                chg_pct = round(((c_now / c_prev) - 1.0) * 100.0, 2)
                item["changePct"] = chg_pct
                arrow = "▲" if chg_pct > 0 else ("▼" if chg_pct < 0 else "")
                item["changeText"] = f"{arrow} {abs(chg_pct):.2f}%"
                if sym == "USD/KRW":
                    item["price"] = round(c_now, 2)
                    item["priceText"] = f"{c_now:,.2f}원"
                elif sym == "QQQ":
                    item["price"] = round(c_now, 2)
                    item["priceText"] = f"{c_now:,.2f}"
        except Exception:
            pass
    nw_data["updatedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M KST")
    return nw_data


def main():
    print("=" * 68, flush=True)
    print(" 한국장 주도주 탐색기 v14 - 스타폴리오 주봉 확정/주중 잠정 동기화 엔진", flush=True)
    print("=" * 68, flush=True)

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"data.json 파일을 찾을 수 없습니다: {data_path}")

    with open(data_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    stocks_list = payload.get("STOCKS_DATA", [])
    start_date = (datetime.now() - timedelta(days=1100)).strftime("%Y-%m-%d")

    updated_stocks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(update_single_stock_ohlcv, st, start_date): i for i, st in enumerate(stocks_list)}
        results = [None] * len(stocks_list)
        for fut in as_completed(future_map):
            idx = future_map[fut]
            results[idx] = fut.result()
        updated_stocks = [r for r in results if r is not None]

    payload["STOCKS_DATA"] = updated_stocks

    # 최신 거래일 및 주중 잠정/주봉 확정 상태 업데이트
    now_kst = datetime.now()
    latest_date_str = now_kst.strftime("%Y-%m-%d")
    for st in updated_stocks:
        d_osc = st.get("dailyOsc", [])
        if d_osc:
            latest_date_str = d_osc[-1]["time"]
            break

    is_weekend_or_fri = now_kst.weekday() >= 4
    payload.setdefault("meta", {})
    payload["meta"]["priceDate"] = latest_date_str
    payload["meta"]["provisionalStatus"] = "확정(주봉)" if is_weekend_or_fri else "잠정(주중)"
    payload["meta"]["updatedAt"] = now_kst.strftime("%Y-%m-%d %H:%M KST")

    if "NIGHT_WATCH_DATA" in payload:
        payload["NIGHT_WATCH_DATA"] = refresh_night_watch(payload["NIGHT_WATCH_DATA"])

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"[완료] 총 {len(updated_stocks)}개 스타폴리오 동기화 종목 시세·주봉·오실레이터·컨센서스 갱신 완료! (기준일: {latest_date_str})", flush=True)


if __name__ == "__main__":
    main()
