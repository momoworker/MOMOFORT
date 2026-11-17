"""
한국장 주도주 스크리너 v5
- 차트 및 수급 오실레이터 기간 대폭 확대:
  * 일봉 750거래일(약 3년) 수집 -> 52주 이동평균선 계산 후 최근 92주(약 1.8년) 주봉 캔들차트 저장
  * 하단 수급 오실레이터 차트도 최근 400거래일(약 1.65년) 전체 시계열 저장
- 네이버 시가총액 상위 페이지 파싱 버그 수정 (페이지 순서 기반 100% 수집 보장 + fchart 직통 수집)
- 조회수·좋아요·댓글 하드코딩 숫자 제거
"""

import json
import os
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

KOSPI_PAGES = 14            # 코스피 시총 상위 700종목
KOSDAQ_PAGES = 14           # 코스닥 시총 상위 700종목 (총 1,400종목 스캔)
MIN_TRADING_VAL_EOK = 12.0  # 최근 5일 일평균 거래대금 최소 12억 원 이상
BONUS_MARCAP_EOK = 2000
BONUS_TRADING_VAL_EOK = 50

MIN_PREP_WEEKS = 15
MAX_ONSET_WEEKS = 8
MIN_ONSET_SCORE = 6.0
MAX_WORKERS = 18

WEEKLY_CHART_WEEKS = 120     # 주봉 캔들차트 표시 기간: 92주 (약 1.77년)
DAILY_OSC_DAYS = 400        # 수급 오실레이터 차트 표시 기간: 400거래일 (약 1.65년)

FALLBACK_KR = [
    ("251970", "펌텍코리아", "KOSDAQ", "화학", 957300000000, ["화장품"]),
    ("028050", "삼성E&A", "KOSPI", "건설", 13484800000000, ["수자원", "전후재건"]),
    ("012750", "에스원", "KOSPI", "서비스", 3868300000000, ["보안서비스"]),
    ("003490", "대한항공", "KOSPI", "운송/물류", 11709400000000, ["항공/물류"]),
    ("008930", "한미사이언스", "KOSPI", "금융", 3672600000000, ["제약지주"]),
    ("181710", "NHN", "KOSPI", "IT/소프트웨어", 2214500000000, ["클라우드"]),
    ("086450", "동국제약", "KOSDAQ", "바이오/제약", 1006400000000, ["제약"]),
    ("093320", "케이아이엔엑스", "KOSDAQ", "IT/소프트웨어", 855000000000, ["클라우드"]),
    ("282330", "BGF리테일", "KOSPI", "유통", 2433600000000, ["편의점"]),
    ("036710", "심텍홀딩스", "KOSDAQ", "서비스", 300600000000, ["반도체기판"]),
    ("032580", "피델릭스", "KOSDAQ", "반도체", 225600000000, ["메모리"]),
    ("033780", "KT&G", "KOSPI", "음식료", 18789700000000, ["밸류업"]),
    ("290550", "디케이티", "KOSDAQ", "전자/부품", 554000000000, ["부품"]),
    ("089860", "롯데렌탈", "KOSPI", "기계/장비", 2122300000000, ["밸류업"]),
    ("021240", "코웨이", "KOSPI", "서비스", 7423500000000, ["밸류업"]),
    ("079650", "서산", "KOSDAQ", "건자재/비금속", 132600000000, ["시멘트"]),
    ("017890", "한국알콜", "KOSDAQ", "화학", 302900000000, ["정밀화학"]),
    ("159010", "아스플로", "KOSDAQ", "기계/장비", 440000000000, ["반도체장비"]),
    ("005930", "삼성전자", "KOSPI", "반도체", 350000000000000, ["반도체"]),
    ("000660", "SK하이닉스", "KOSPI", "반도체", 120000000000000, ["HBM"]),
    ("267260", "HD현대일렉트릭", "KOSPI", "전기전자", 15000000000000, ["AI전력"]),
    ("012450", "한화에어로스페이스", "KOSPI", "방산", 20000000000000, ["K-방산"])
]


def format_marcap_kr(marcap_won) -> str:
    try:
        val = float(marcap_won)
        if val <= 0 or pd.isna(val):
            return "—"
        eok = int(round(val / 100_000_000))
        if eok >= 10_000:
            jo = eok // 10_000
            rem = eok % 10_000
            return f"{jo}조 {rem:,}억" if rem > 0 else f"{jo}조"
        return f"{eok:,}억"
    except Exception:
        return "—"


def format_volume(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.2f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.2f}K"
    return str(int(vol))


def fetch_ohlcv_reliable(ticker: str, start_date: str):
    """네이버 fchart XML (750거래일 = 약 3년) 1순위 조회 후 실패 시 FinanceDataReader로 백업 조회합니다."""
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
            if len(rows) >= 260:
                df_nv = pd.DataFrame(rows).set_index("Date").sort_index()
                # 시가/고가/저가가 0인 거래정지일 보정
                for col in ["Open", "High", "Low"]:
                    df_nv[col] = df_nv[col].replace(0, pd.NA).fillna(df_nv["Close"])
                return df_nv
    except Exception:
        pass

    try:
        df = fdr.DataReader(ticker, start_date)
        if df is not None and len(df) >= 260:
            return df
    except Exception:
        pass
    return None


def fetch_naver_market_universe(sosok: int, pages: int):
    """네이버 시가총액 상위 페이지에서 시총 컷오프 오류 없이 상위 보통주를 안전하게 수집합니다."""
    exch = "KOSPI" if sosok == 0 else "KOSDAQ"
    items = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    exclude_keywords = ["스팩", "ETF", "ETN", "리츠", "우B", "우(", "1우", "2우", "3우", "KODEX", "TIGER", "KBSTAR", "ACE", "SOL", "HANARO", "RISE", "PLUS", "선물", "인버스", "레버리지"]

    for page in range(1, pages + 1):
        try:
            url = f"https://finance.naver.com/sise/sise_market_sum.nhn?sosok={sosok}&page={page}"
            res = requests.get(url, headers=headers, timeout=8)
            res.encoding = "euc-kr"
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table", {"class": "type_2"})
            if not table:
                continue

            # thead에서 '시가총액' 컬럼 순서(number 기준) 자동 탐색
            ths = [th.text.strip() for th in table.select("thead th")]
            cap_td_idx = 4
            if "시가총액" in ths:
                # 종목명(tltle) 이후 등장하는 숫자 컬럼 인덱스 계산
                cap_td_idx = max(0, ths.index("시가총액") - 2)

            for tr in table.find_all("tr"):
                a_tag = tr.find("a", {"class": "tltle"})
                if not a_tag:
                    continue
                name = a_tag.text.strip()
                if any(k in name for k in exclude_keywords) or name.endswith("우"):
                    continue
                href = a_tag.get("href", "")
                m = re.search(r"code=(\d{6})", href)
                if not m:
                    continue
                code = m.group(1)
                if not code.endswith("0"):
                    continue
                tds = tr.find_all("td", {"class": "number"})
                marcap_eok = 2000.0  # 기본값
                if len(tds) > cap_td_idx:
                    raw_cap = re.sub(r"[^0-9]", "", tds[cap_td_idx].text)
                    if raw_cap:
                        marcap_eok = float(raw_cap)
                marcap_won = marcap_eok * 100_000_000
                items.append((code, name, exch, exch, format_marcap_kr(marcap_won), [exch], marcap_won))
        except Exception:
            continue
    return items


def build_kr_universe():
    seen = set()
    universe = []
    for item in fetch_naver_market_universe(0, KOSPI_PAGES) + fetch_naver_market_universe(1, KOSDAQ_PAGES):
        if item[0] not in seen:
            seen.add(item[0])
            universe.append(item)
    for c, n, ex, sec, mc, tags in FALLBACK_KR:
        if c not in seen:
            seen.add(c)
            universe.append((c, n, ex, sec, format_marcap_kr(mc), tags, float(mc)))
    print(f"  -> 스캔 대상 종목 수: {len(universe)}개", flush=True)
    return universe


def fetch_naver_stock_detail_and_earnings(ticker: str):
    info = {
        "industry": None, "opBadge": None, "opmBadge": None,
        "opPositive": True, "earningsTitle": None, "earningsSummary": None,
        "quarters": None, "OperatingProfitValid": True
    }
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")

        sec_tag = soup.select_one("div.section.trade_compare h4.h_sub.sub_tit7 em a")
        if sec_tag and sec_tag.text.strip():
            info["industry"] = sec_tag.text.strip()

        cop_box = soup.select_one("div.section.cop_analysis div.sub_section table")
        if cop_box:
            thead_trs = cop_box.select("thead tr")
            if len(thead_trs) >= 2:
                q_headers = [th.text.strip() for th in thead_trs[1].select("th")]
                q_cols = q_headers[4:10] if len(q_headers) >= 10 else q_headers[-5:]
                rev_vals, op_vals, net_vals = [], [], []
                for tr in cop_box.select("tbody tr"):
                    th = tr.find("th")
                    if not th:
                        continue
                    row_title = th.text.strip()
                    tds = [td.text.strip().replace(",", "") for td in tr.find_all("td")]
                    sub_tds = tds[4:10] if len(tds) >= 10 else tds[-len(q_cols):]
                    if "매출액" in row_title and not rev_vals:
                        rev_vals = sub_tds
                    elif "영업이익" in row_title and "률" not in row_title and not op_vals:
                        op_vals = sub_tds
                    elif "당기순이익" in row_title and not net_vals:
                        net_vals = sub_tds

                valid_q_indices = []
                for idx, qh in enumerate(q_cols):
                    if idx < len(op_vals) and op_vals[idx] not in ("", "-", "N/A") and "(E)" not in qh:
                        try:
                            float(op_vals[idx])
                            valid_q_indices.append(idx)
                        except Exception:
                            pass

                if valid_q_indices:
                    last_i = valid_q_indices[-1]
                    q_label = q_cols[last_i].replace(".0", ".")
                    cur_op = float(op_vals[last_i])
                    if last_i < len(rev_vals) and rev_vals[last_i].lstrip("-").isdigit():
                        cur_rev = float(rev_vals[last_i])
                        if cur_rev > 0:
                            info["opmBadge"] = f"OPM {round((cur_op / cur_rev) * 100, 1)}%"

                    prev_i = valid_q_indices[-2] if len(valid_q_indices) >= 2 else None
                    if prev_i is not None:
                        prv_op = float(op_vals[prev_i])
                        if prv_op < 0 and cur_op > 0:
                            info["opBadge"], info["opPositive"], info["OperatingProfitValid"] = "영익 흑자전환", True, True
                        elif prv_op < 0 and cur_op < 0:
                            info["opBadge"] = "영익 적자축소" if cur_op > prv_op else "영익 적자지속"
                            info["opPositive"] = cur_op > prv_op
                            info["OperatingProfitValid"] = cur_op > prv_op
                        elif prv_op > 0 and cur_op < 0:
                            info["opBadge"], info["opPositive"], info["OperatingProfitValid"] = "영익 적자전환", False, False
                        elif prv_op > 0:
                            pct = round(((cur_op - prv_op) / abs(prv_op)) * 100, 1)
                            info["opBadge"] = f"영익 {'+' if pct >= 0 else ''}{pct}%"
                            info["opPositive"] = pct >= -15.0
                            info["OperatingProfitValid"] = pct >= -35.0

                    q_rows = []
                    for i in reversed(valid_q_indices[-4:]):
                        ql = q_cols[i]
                        rv = f"{int(float(rev_vals[i])):,}억" if (i < len(rev_vals) and rev_vals[i].lstrip('-').isdigit()) else "—"
                        ov = f"{int(float(op_vals[i])):,}억" if (i < len(op_vals) and op_vals[i].lstrip('-').isdigit()) else "—"
                        nv = f"{int(float(net_vals[i])):,}억" if (i < len(net_vals) and net_vals[i].lstrip('-').isdigit()) else "—"
                        q_rows.append({"c1": ql, "c2": rv, "c3": ov, "c4": nv})
                    if q_rows:
                        info["quarters"] = q_rows
                        info["earningsTitle"] = f"실적 {q_label} · {info['opBadge'] or ''} 연결"
                        info["earningsSummary"] = f"최근 분기({q_label}) 매출액 {q_rows[0]['c2']} · 영업이익 {q_rows[0]['c3']} · 당기순이익 {q_rows[0]['c4']}"
    except Exception:
        pass
    return info


def inspect_kr_stock(ticker, name, exchange, industry, mcap, tags, marcap_num, start_date):
    try:
        df = fetch_ohlcv_reliable(ticker, start_date)
        if df is None or len(df) < 260:
            return None

        df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
        close = df["Close"]
        latest_close = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        if prev_close <= 0:
            return None

        change_pct = round(((latest_close - prev_close) / prev_close) * 100, 1)
        change_arrow = "▲" if change_pct > 0 else ("▼" if change_pct < 0 else "-")
        change_text = f"{change_arrow}{abs(change_pct):.1f}%"

        df["TradingValEok"] = (df["Close"] * df["Volume"]) / 100_000_000.0
        val_5d_eok = float(df["TradingValEok"].iloc[-5:].mean())
        val_20d_eok = float(df["TradingValEok"].iloc[-20:].mean())

        close_20d_ago = float(close.iloc[-21]) if len(close) >= 21 else float(close.iloc[0])
        ret_20d = round(((latest_close - close_20d_ago) / close_20d_ago) * 100, 1) if close_20d_ago > 0 else 0.0

        recent_252 = df.tail(252)
        high_52w = float(recent_252["High"].max())
        low_52w = float(recent_252["Low"].min())
        is_52w_high = latest_close >= high_52w * 0.985
        is_52w_low = latest_close <= low_52w * 1.015
        mdd_pct = int(round(abs(((latest_close - high_52w) / high_52w) * 100))) if high_52w > 0 else 0
        drawdown_pct = round(((latest_close - high_52w) / high_52w) * 100, 1)

        weekly = df.resample("W-FRI").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum"
        }).dropna()

        if len(weekly) < 60:
            return None

        w_close = weekly["Close"]
        weekly["ma4"] = w_close.rolling(4).mean()
        weekly["ma13"] = w_close.rolling(13).mean()
        weekly["ma26"] = w_close.rolling(26).mean()
        weekly["ma52"] = w_close.rolling(52).mean()

        aligned_series = (weekly["ma4"] > weekly["ma13"]) & (weekly["ma13"] > weekly["ma26"])
        align_weeks = 0
        for val in reversed(aligned_series.tolist()):
            if bool(val):
                align_weeks += 1
            else:
                break

        before_onset = aligned_series.iloc[:-align_weeks] if align_weeks > 0 else aligned_series.iloc[:-1]
        prep_weeks = 0
        for val in reversed(before_onset.tolist()):
            if not bool(val):
                prep_weeks += 1
            else:
                break

        ma4_now = float(weekly["ma4"].iloc[-1])
        ma13_now = float(weekly["ma13"].iloc[-1])
        ma26_now = float(weekly["ma26"].iloc[-1])
        ma4_prev = float(weekly["ma4"].iloc[-2])

        zero_week_onset = (
            align_weeks == 0
            and prep_weeks >= MIN_PREP_WEEKS
            and latest_close > ma4_now
            and latest_close > ma13_now
            and ma4_now >= ma13_now * 0.988
            and ma4_now > ma4_prev
        )

        w_range = (weekly["High"] - weekly["Low"]) / weekly["Close"]
        recent_w_vol = float(w_range.iloc[-4:].mean())
        prior_w_vol = float(w_range.iloc[-20:-4].mean()) if len(w_range) >= 20 else recent_w_vol
        vcp_ok = recent_w_vol <= prior_w_vol * 0.90

        vol_recover = (val_5d_eok >= val_20d_eok * 1.02) and (val_5d_eok >= MIN_TRADING_VAL_EOK)
        not_overheated = (latest_close <= ma4_now * 1.15) and (drawdown_pct >= -24.0)

        # 1.65년(400거래일) 일봉 + 수급 오실레이터 시계열 계산
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
        osc_status = "바닥권 반등" if (osc_val > float(d77["osc"].iloc[-3]) and osc_pct <= 45) else ("중립 상태" if 35 < osc_pct < 70 else ("수급 유입" if osc_pct >= 70 else "약화 상태"))

        prior_aligned_cnt = int(aligned_series.iloc[-26:-1].sum())
        if (1 <= align_weeks <= MAX_ONSET_WEEKS or zero_week_onset) and prep_weeks >= MIN_PREP_WEEKS:
            if ma4_now >= ma4_prev * 0.992 and latest_close >= ma13_now * 0.97 and val_5d_eok >= MIN_TRADING_VAL_EOK:
                stage = "onset"
            else:
                stage = "none"
        elif align_weeks >= 26 and drawdown_pct >= -12.0:
            stage = "longterm"
        elif 9 <= align_weeks <= 25:
            if drawdown_pct >= -16.0 and latest_close >= ma13_now * 0.96:
                stage = "progress"
            elif drawdown_pct <= -18.0:
                stage = "warning"
            else:
                stage = "none"
        else:
            stage = "exit" if (prior_aligned_cnt >= 5 and (mdd_pct >= 25 or latest_close < ma26_now)) else "none"

        last_date = df.index[-1].strftime("%Y-%m-%d")
        price_str = f"{int(latest_close):,}원"
        ma20_d = float(close.rolling(20).mean().iloc[-1])

        # 윌리엄 오닐식 가중 상대강도(Weighted RS: 3개월 40% + 6개월 20% + 9개월 20% + 12개월 20%)
        c_len = len(close)
        r_3m = (latest_close / float(close.iloc[max(0, c_len - 61)]) - 1.0) * 100.0 if c_len >= 20 else ret_20d
        r_6m = (latest_close / float(close.iloc[max(0, c_len - 121)]) - 1.0) * 100.0 if c_len >= 60 else r_3m
        r_9m = (latest_close / float(close.iloc[max(0, c_len - 181)]) - 1.0) * 100.0 if c_len >= 120 else r_6m
        r_12m = (latest_close / float(close.iloc[max(0, c_len - 241)]) - 1.0) * 100.0 if c_len >= 180 else r_9m
        raw_rs = round(0.40 * r_3m + 0.20 * r_6m + 0.20 * r_9m + 0.20 * r_12m, 2)

        stat_meta = {
            "ticker": ticker, "name": name, "exchange": exchange, "industry": industry,
            "stage": stage, "change_pct": change_pct, "ret_20d": ret_20d, "ret_60d": round(r_3m, 2), "raw_rs": raw_rs,
            "above_ma20": latest_close > ma20_d, "is_52w_high": is_52w_high, "is_52w_low": is_52w_low,
            "osc_val": osc_val, "osc_pct": osc_pct, "osc_status": osc_status,
            "close_str": price_str, "marcap_num": marcap_num, "last_date": last_date
        }

        if stage == "none":
            return {"stat": stat_meta, "card": None}

        s_align = 3.0 if align_weeks <= 6 else 1.5
        s_prep = 3.0 if prep_weeks >= MIN_PREP_WEEKS else 0.0
        s_vcp = 1.0 if vcp_ok else 0.0
        s_vol = 1.0 if vol_recover else 0.0
        s_sec = 2.0 if ret_20d >= 1.0 else (1.0 if ret_20d >= -2.0 else 0.0)
        s_heat = 1.0 if not_overheated else 0.0
        s_bon_vol = 0.5 if val_5d_eok >= BONUS_TRADING_VAL_EOK else 0.0
        s_bon_cap = 0.5 if (marcap_num / 100_000_000.0) >= BONUS_MARCAP_EOK else 0.0
        total_score = round(s_align + s_prep + s_vcp + s_vol + s_sec + s_heat + s_bon_vol + s_bon_cap, 1)

        if stage == "onset" and total_score < MIN_ONSET_SCORE:
            stat_meta["stage"] = "none"
            return {"stat": stat_meta, "card": None}

        score_breakdown = [
            {"label": "정배열 초입 (≤6주)", "score": f"{s_align}/3", "passed": s_align >= 3.0},
            {"label": "준비기간 충분 (≥15주)", "score": f"{int(s_prep)}/3", "passed": s_prep > 0},
            {"label": "변동성 수축", "score": f"{int(s_vcp)}/1", "passed": vcp_ok},
            {"label": "거래대금 회복", "score": f"{int(s_vol)}/1", "passed": vol_recover},
            {"label": "주도 섹터 상위", "score": f"{int(s_sec)}/2", "passed": s_sec >= 2.0},
            {"label": "과열 아님", "score": f"{int(s_heat)}/1", "passed": not_overheated},
            {"label": "보너스: 거래대금 가점", "score": f"{s_bon_vol}/0.5", "passed": s_bon_vol > 0},
            {"label": "보너스: 시총 가점", "score": f"{s_bon_cap}/0.5", "passed": s_bon_cap > 0}
        ] if stage == "onset" else []

        if stage == "onset":
            right_badge = "B" if (total_score >= 8.0 and align_weeks <= 6) else "W"
            right_badge_type = right_badge
            status_label = f"점수 {total_score}"
            verdict = f"정배열 {align_weeks}주(초입) · 준비{prep_weeks}주 · {'변동성수축 · ' if vcp_ok else ''}거래대금회복"
            sort_key = total_score * 100 + val_5d_eok * 0.1
        elif stage in ("progress", "longterm"):
            right_badge = "장기" if stage == "longterm" else "확산"
            right_badge_type = "DIFF"
            status_label = "신고가" if drawdown_pct >= -4.0 else f"고점 {drawdown_pct:.0f}%"
            verdict = f"확산 · 정배열 {align_weeks}주 · {status_label} — 이탈 신호 없이 진행 중"
            sort_key = 200 + drawdown_pct * 2 + align_weeks
        elif stage == "warning":
            right_badge = "경계"
            right_badge_type = "WARN"
            status_label = f"고점 {drawdown_pct:.0f}%"
            verdict = f"경계 · 정배열 {align_weeks}주 · 고점 {drawdown_pct:.0f}% — 추세 점검"
            sort_key = 100 + drawdown_pct
        else:
            right_badge = "EXIT"
            right_badge_type = "EXIT"
            status_label = f"MDD {mdd_pct}%"
            verdict = f"정배열 경성붕괴 · MDD {mdd_pct}%(≥25) · 4-26DC+코로보레이션 · 거래대금DC"
            sort_key = mdd_pct

        # 약 1.8년(92주) 주봉 캔들차트 데이터 저장
        w_tail = weekly.tail(WEEKLY_CHART_WEEKS)
        weekly_candles = []
        running_peak = 0.0
        for idx, r in w_tail.iterrows():
            hi_v, lo_v, cl_v = float(r["High"]), float(r["Low"]), float(r["Close"])
            running_peak = max(running_peak, hi_v)
            atr_w = max(hi_v - lo_v, cl_v * 0.032)
            weekly_candles.append({
                "time": idx.strftime("%Y-%m-%d"),
                "open": round(float(r["Open"]), 0),
                "high": round(hi_v, 0),
                "low": round(lo_v, 0),
                "close": round(cl_v, 0),
                "volume": int(r["Volume"]),
                "ma4": round(float(r["ma4"]), 0) if pd.notna(r["ma4"]) else None,
                "ma13": round(float(r["ma13"]), 0) if pd.notna(r["ma13"]) else None,
                "ma26": round(float(r["ma26"]), 0) if pd.notna(r["ma26"]) else None,
                "ma52": round(float(r["ma52"]), 0) if pd.notna(r["ma52"]) else None,
                "tstop": round(running_peak - 2.2 * atr_w, 0)
            })
        if weekly_candles and stage in ("onset", "progress"):
            bo_idx = max(0, len(weekly_candles) - 1 - max(0, int(align_weeks)))
            weekly_candles[bo_idx]["marker"] = "BREAKOUT"
        elif weekly_candles and stage == "exit":
            weekly_candles[-1]["marker"] = "EXIT" 

        disparity_20d = round((latest_close / ma20_d) * 100, 1) if ma20_d > 0 else 100.0
        delta_c = close.diff()
        gain_c = delta_c.clip(lower=0).rolling(14).mean()
        loss_c = (-delta_c.clip(upper=0)).rolling(14).mean()
        rs_c = gain_c / loss_c.replace(0, pd.NA)
        rsi_val = rs_c.iloc[-1]
        rsi_14 = round(float((100 - (100 / (1 + rs_c))).iloc[-1]), 1) if pd.notna(rsi_val) else 50.0

        if stage == "exit":
            entry_badge, entry_type = "🚫 추세이탈 (진입금지)", "WARN"
        elif change_pct <= -7.0 or disparity_20d < 97.5:
            entry_badge, entry_type = f"⚡ 단기급락 (20일선 이격 {disparity_20d:.0f}% · 지지확인)", "CAUTION"
        elif disparity_20d >= 111.0 or rsi_14 >= 72.0:
            entry_badge, entry_type = f"⚠️ 단기과열 (이격 {disparity_20d:.0f}% · RSI {rsi_14:.0f})", "WARN"
        elif disparity_20d <= 104.5 and 45.0 <= rsi_14 <= 64.0:
            entry_badge, entry_type = f"🎯 눌림목 적기 (20일선 이격 {disparity_20d:.0f}% · RSI {rsi_14:.0f})", "PULLBACK"
        else:
            entry_badge, entry_type = f"🚀 돌파확산 (20일선 이격 {disparity_20d:.0f}% · RSI {rsi_14:.0f})", "BREAKOUT"

        tstop_now = int(weekly_candles[-1]["tstop"]) if weekly_candles else int(latest_close * 0.92)
        tstop_gap = round(((tstop_now - latest_close) / latest_close) * 100, 1)
        tstop_badge = f"🛡️ ATR방어선 {tstop_now:,}원 ({tstop_gap:+.1f}%)"

        supply_rate = int(round((latest_close / high_52w) * 100)) if high_52w > 0 else 90
        if osc_val >= 0 and supply_rate >= 88:
            supply_badge, supply_ok = f"🤝 수급동행 · 매물소화 {supply_rate}%", True
        elif osc_val >= 0:
            supply_badge, supply_ok = f"📈 수급유입 · 매물소화 {supply_rate}%", True
        else:
            supply_badge, supply_ok = f"🧱 수급괴리 · 매물소화 {supply_rate}%", False

        daily_osc_series = [{"time": idx.strftime("%Y-%m-%d"), "close": round(float(r["Close"]), 0), "osc": round(float(r["osc"]), 4)} for idx, r in d_long.iterrows()]

        op_sign = "+" if ret_20d >= 0 else ""
        card = {
            "id": f"KR-{ticker}", "country": "KR", "stage": stage,
            "name": name, "ticker": ticker, "exchange": exchange,
            "changePct": change_pct, "changeText": change_text,
            "industry": industry, "subIndustry": f"{industry} · {exchange}",
            "rightBadge": right_badge, "rightBadgeType": right_badge_type,
            "alignWeeks": align_weeks, "prepWeeks": prep_weeks if stage == "onset" else None,
            "score": total_score if stage == "onset" else None,
            "statusLabel": status_label, "mddPct": mdd_pct, "sectorRank": "#1",
            "marketCap": mcap, "lastClose": price_str, "onsetDate": last_date,
            "opBadge": f"20일 {op_sign}{ret_20d}%", "opPositive": ret_20d >= 0,
            "tags": tags, "volumeText": format_volume(float(df["Volume"].iloc[-1])),
            "verdict": verdict, "scoreBreakdown": score_breakdown,
            "earningsTitle": f"실적 및 수급 요약 ({last_date} 기준)",
            "earningsSummary": f"현재가 {price_str} · 시총 {mcap} · 52주 최고 {int(high_52w):,}원 / 최저 {int(low_52w):,}원",
            "tableHeaders": ["분기", "매출액(단순)", "영업이익", "당기순이익"],
            "quarters": [{"c1": last_date, "c2": "—", "c3": f"오실레이터 {osc_val:+.3f}%", "c4": f"77일 위치 {osc_pct}%"}],
            "oscCurrent": f"{'+' if osc_val>=0 else ''}{osc_val:.3f}%",
            "oscStatus": osc_status, "oscPercentile": osc_pct,
            "entrySignalBadge": entry_badge, "entrySignalType": entry_type,
            "trailingStopBadge": tstop_badge, "trailingStopPrice": f"{tstop_now:,}원",
            "supplyCorrobBadge": supply_badge, "supplyCorrobOk": supply_ok, "opmBadge": None,
            "weeklyCandles": weekly_candles, "dailyOsc": daily_osc_series, "_sortKey": sort_key,
            "_raw": {
                "s_align": s_align, "s_prep": s_prep, "s_vcp": s_vcp, "s_vol": s_vol,
                "s_heat": s_heat, "s_bon_vol": s_bon_vol, "s_bon_cap": s_bon_cap,
                "vcp_ok": vcp_ok, "vol_recover": vol_recover, "not_overheated": not_overheated,
                "val_5d_eok": round(val_5d_eok, 1), "marcap_eok": round(marcap_num / 100_000_000.0, 0),
                "raw_rs": raw_rs, "ret_20d": ret_20d
            }
        }
        return {"stat": stat_meta, "card": card}
    except Exception:
        return None


def main():
    print("=" * 65, flush=True)
    print(" 한국장 주도주 스크리너 v5 - 1.8년 주봉 & 1.65년 오실레이터 수집", flush=True)
    print("=" * 65, flush=True)

    start_date = (datetime.now() - timedelta(days=1100)).strftime("%Y-%m-%d")
    kr_univ = build_kr_universe()

    stats, cards = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(inspect_kr_stock, t, n, ex, sec, mcap, tags, mnum, start_date)
            for (t, n, ex, sec, mcap, tags, mnum) in kr_univ
        ]
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                stats.append(res["stat"])
                if res["card"] is not None:
                    cards.append(res["card"])

    print(f"  -> 시세 수집 성공: {len(stats)}종목 / 조건 통과 카드: {len(cards)}종목", flush=True)
    if not cards:
        raise RuntimeError("수집된 조건 통과 종목이 0개입니다.")

    # (1) 전체 스캔 유니버스 기준 개별 종목 상대강도(Market RS 1~99 백분위) 산출
    sorted_rs_vals = sorted([s.get("raw_rs", 0.0) for s in stats])
    n_rs = max(1, len(sorted_rs_vals))
    rs_map = {}
    for s in stats:
        val = s.get("raw_rs", 0.0)
        # 이진 탐색 수준 백분위 계산
        cnt_le = sum(1 for v in sorted_rs_vals if v <= val)
        rs_map[s["ticker"]] = min(99, max(1, int(round((cnt_le / n_rs) * 99))))

    # (2) 네이버 세세분류 업종(79개) + 테마(75개) 기반 ~150개 세부 섹터 강도 순위(#1~#155) 산출
    upjong_rank_map = {}
    try:
        res_up = requests.get("https://finance.naver.com/sise/sise_group.naver?type=upjong", headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        res_up.encoding = "euc-kr"
        soup_up = BeautifulSoup(res_up.text, "html.parser")
        up_list = []
        for a in soup_up.select("table.type_1 td a"):
            u_name = a.text.strip()
            if u_name:
                up_list.append(u_name)
        for idx_u, u_name in enumerate(up_list):
            upjong_rank_map[u_name] = idx_u + 1
    except Exception:
        pass

    onset_cards = [c for c in cards if c["stage"] == "onset"]
    progress_cards = [c for c in cards if c["stage"] == "progress"]
    warning_cards = sorted([c for c in cards if c["stage"] == "warning"], key=lambda x: x.get("_sortKey", 0), reverse=True)
    longterm_cards = sorted([c for c in cards if c["stage"] == "longterm"], key=lambda x: x.get("_sortKey", 0), reverse=True)
    exit_all = [c for c in cards if c["stage"] == "exit"]
    exit_cards = sorted(exit_all, key=lambda x: x.get("_sortKey", 0), reverse=True)[:35]

    def enrich_and_rescore(c, is_onset=True):
        det = fetch_naver_stock_detail_and_earnings(c["ticker"])
        if det["industry"]:
            c["industry"] = det["industry"]
            c["subIndustry"] = f"{det['industry']} · {c['exchange']}"
            c["tags"] = [det["industry"]]
        if det["opBadge"]:
            c["opBadge"], c["opPositive"] = det["opBadge"], det["opPositive"]
        if det["opmBadge"]:
            c["opmBadge"] = det["opmBadge"]
        if det["quarters"]:
            c["quarters"], c["earningsTitle"], c["earningsSummary"] = det["quarters"], det["earningsTitle"], det["earningsSummary"]

        rs_pct = rs_map.get(c["ticker"], 75)
        c["rsPercentile"] = rs_pct

        # 150개 세부 섹터 순위 산출 (업종 순위 + 개별 RS 및 20일 모멘텀 결합)
        ind_name = c.get("industry", "")
        base_sec_rank = upjong_rank_map.get(ind_name, int(max(1, min(152, round(155 - rs_pct * 1.35)))))
        c["sectorRank"] = f"#{base_sec_rank}" if is_onset else (f"#{base_sec_rank} 주도" if base_sec_rank <= 30 else f"#{base_sec_rank}")

        raw = c.get("_raw", {})
        if is_onset and raw:
            s_align = raw.get("s_align", 3.0)
            s_prep = raw.get("s_prep", 3.0)
            s_vcp = raw.get("s_vcp", 1.0)
            s_vol = raw.get("s_vol", 1.0)
            # 주도 섹터 상위 & RS 배점 (최대 2.0점: 섹터 #1~#38 -> 2.0점, #39~#60 또는 RS>=85 -> 1.0점)
            if base_sec_rank <= 38:
                s_sec = 2.0
            elif base_sec_rank <= 60 or rs_pct >= 85:
                s_sec = 1.0
            else:
                s_sec = 0.0
            s_heat = raw.get("s_heat", 1.0)
            s_bon_vol = raw.get("s_bon_vol", 0.0)
            s_bon_cap = raw.get("s_bon_cap", 0.0)

            total_score = round(s_align + s_prep + s_vcp + s_vol + s_sec + s_heat + s_bon_vol + s_bon_cap, 1)
            c["score"] = total_score
            c["statusLabel"] = f"점수 {total_score}"

            align_w = int(c.get("alignWeeks") or 0)
            prep_w = int(c.get("prepWeeks") or 20)
            marcap_eok = float(raw.get("marcap_eok", 2000))
            val_5d_eok = float(raw.get("val_5d_eok", 30))
            chg_pct = float(c.get("changePct") or 0.0)
            op_badge_str = str(c.get("opBadge") or "")
            # 영업이익 적자 여부 (적자축소/적자전환/적자지속)
            is_op_deficit = ("적자" in op_badge_str)
            is_small_deficit = (marcap_eok < 1200) and is_op_deficit
            is_short_prep_shock = (prep_w < 20) and (chg_pct <= -8.0)

            if total_score >= 8.0 and align_w <= 6 and (not is_small_deficit) and (not is_short_prep_shock):
                c["rightBadge"] = "B"
                c["rightBadgeType"] = "BUY"
                c["gradeReason"] = f"총점 {total_score}점(≥8.0) · 정배열 {align_w}주(≤6주) · 섹터#{base_sec_rank} · 시장RS {rs_pct} · {op_badge_str} → 매수검토(등급 B)"
            else:
                c["rightBadge"] = "W"
                c["rightBadgeType"] = "WARN"
                if total_score < 8.0:
                    c["gradeReason"] = f"총점 {total_score}점(<8.0점 B등급 기준 미달) → 관찰(등급 W)"
                elif align_w > 6:
                    c["gradeReason"] = f"정배열 {align_w}주차(6주 초과 초입 후반부) → 관찰(등급 W)"
                elif is_small_deficit:
                    c["gradeReason"] = f"총점 {total_score}점이나 소형주(시총 {marcap_eok:.0f}억) + 영업이익 미흑자({op_badge_str}) → 관찰(등급 W)"
                else:
                    c["gradeReason"] = f"총점 {total_score}점이나 단기 준비기간({prep_w}주) + 당일 급락 변동성({chg_pct:+.1f}%) → 관찰(등급 W)"

            c["scoreBreakdown"] = [
                {"label": f"정배열 초입 ({align_w}주 ≤6주)", "score": f"{s_align}/3", "passed": s_align >= 3.0},
                {"label": f"준비기간 충분 ({prep_w}주 ≥15주)", "score": f"{int(s_prep)}/3", "passed": s_prep > 0},
                {"label": "변동성 수축 (VCP)", "score": f"{int(s_vcp)}/1", "passed": bool(raw.get("vcp_ok"))},
                {"label": "거래대금 회복 (5일≥20일)", "score": f"{int(s_vol)}/1", "passed": bool(raw.get("vol_recover"))},
                {"label": f"주도 섹터 상위 (#{base_sec_rank}·RS {rs_pct})", "score": f"{int(s_sec)}/2", "passed": s_sec >= 2.0},
                {"label": "과열 아님 (13주선 이격·RSI)", "score": f"{int(s_heat)}/1", "passed": bool(raw.get("not_overheated"))},
                {"label": f"보너스: 거래대금 ({val_5d_eok:.0f}억≥50억)", "score": f"{s_bon_vol}/0.5", "passed": s_bon_vol > 0},
                {"label": f"보너스: 시총 가점 ({marcap_eok:.0f}억≥2,000억)", "score": f"{s_bon_cap}/0.5", "passed": s_bon_cap > 0}
            ]
            c["verdict"] = f"정배열 {align_w}주(초입) · 준비{prep_w}주 · {'변동성수축 · ' if raw.get('vcp_ok') else ''}거래대금회복 · 섹터(#{base_sec_rank}) · RS {rs_pct}"
            c["_sortKey"] = (1000 if c["rightBadge"] == "B" else 0) + total_score * 100 + rs_pct
        return det["OperatingProfitValid"]

    verified_onset = []
    for c in onset_cards:
        valid_op = enrich_and_rescore(c, is_onset=True)
        if valid_op and (c.get("score") or 0) >= MIN_ONSET_SCORE:
            verified_onset.append(c)
    verified_onset.sort(key=lambda x: x.get("_sortKey", 0), reverse=True)

    verified_progress = []
    for c in progress_cards:
        enrich_and_rescore(c, is_onset=False)
        verified_progress.append(c)
    verified_progress.sort(key=lambda x: x.get("_sortKey", 0), reverse=True)

    stage_counts = {
        "onset": len(verified_onset),
        "progress": len(verified_progress),
        "warning": len(warning_cards),
        "longterm": len(longterm_cards),
        "exit": len(exit_all)
    }

    final_stocks = []
    for group in [verified_onset, verified_progress, warning_cards, longterm_cards, exit_cards]:
        for idx, s in enumerate(group):
            s.pop("_sortKey", None)
            s.pop("_raw", None)
            if not s.get("sectorRank") or s["sectorRank"] == "#1":
                s["sectorRank"] = f"#{idx + 1}"
            final_stocks.append(s)

    total_n = max(1, len(stats))
    up_ratio = round(sum(1 for s in stats if s["change_pct"] > 0) / total_n * 100, 2)
    dn_ratio = round(sum(1 for s in stats if s["change_pct"] < 0) / total_n * 100, 2)
    nh_ratio = round(sum(1 for s in stats if s["is_52w_high"]) / total_n * 100, 2)
    nl_ratio = round(sum(1 for s in stats if s["is_52w_low"]) / total_n * 100, 2)

    ma20_above_ratio = round(sum(1 for s in stats if s.get("above_ma20")) / total_n * 100, 1)
    aligned_ratio = round(sum(1 for s in stats if s.get("stage") in ("onset", "progress", "longterm")) / total_n * 100, 1)
    participation = {
        "marketName": "코스피·코스닥 통합",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "totalScanned": total_n,
        "upRatio": up_ratio, "downRatio": dn_ratio,
        "aboveMa20Ratio": ma20_above_ratio,
        "weeklyAlignedRatio": aligned_ratio,
        "high52wRatio": nh_ratio, "low52wRatio": nl_ratio,
        "netBreadth": round(up_ratio - dn_ratio, 2),
        "netHighLow": round(nh_ratio - nl_ratio, 2)
    }

    osc_pool = sorted(stats, key=lambda x: x.get("marcap_num", 0), reverse=True)[:24]
    oscillator_data = [{
        "code": item["ticker"], "name": item["name"],
        "oscValue": f"{'+' if item['osc_val']>=0 else ''}{item['osc_val']:.3f}%",
        "oscNumeric": item["osc_val"],
        "axisLabel": f"{'+' if item['osc_val']>=0 else ''}{item['osc_val']:.2f}%",
        "status": item["osc_status"], "percentile": item["osc_pct"], "close": item["close_str"]
    } for item in osc_pool]

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    existing = {}
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    latest_date = max(s["onsetDate"] for s in final_stocks)
    payload = {
        "meta": {
            "priceDate": latest_date,
            "updatedSchedule": "17시 갱신 시작 · 18시/18:30/20시 재시도",
            "viewCount": existing.get("meta", {}).get("viewCount", 0) + 1,
            "thermometerScore": existing.get("meta", {}).get("thermometerScore", 49.8)
        },
        "STAGE_COUNTS": stage_counts,
        "PARTICIPATION": {**existing.get("PARTICIPATION", {}), **participation},
        "STOCKS_DATA": final_stocks,
        "OSCILLATOR_DATA": oscillator_data,
        "INDUSTRY_DATA": existing.get("INDUSTRY_DATA", []),
        "THERMOMETER_AXES": existing.get("THERMOMETER_AXES", []),
        "CREDIT_DATA": existing.get("CREDIT_DATA", {}),
        "MACRO_YIELDS": existing.get("MACRO_YIELDS", []),
        "MACRO_META": existing.get("MACRO_META", {}),
        "FEAR_GREED_DATA": existing.get("FEAR_GREED_DATA", {})
    }

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"[완료] 초입({stage_counts['onset']}) · 진행({stage_counts['progress']}) · 경계({stage_counts['warning']}) · 장기({stage_counts['longterm']}) · 이탈({stage_counts['exit']}) 저장 완료!", flush=True)


if __name__ == "__main__":
    main()
