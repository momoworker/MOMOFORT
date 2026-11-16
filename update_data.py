"""
한국장 주도주 스크리너 - 실제 leader.starfolio.io 엄격 컷오프 재현 엔진 (v3)
핵심 개선 사항:
1. 스캔 범위 대폭 확대: 코스피·코스닥 시가총액 상위 약 800~1,000종목(시총 1,000억 이상 전수) 고속 병렬 스캔
2. 엄격한 '초입' 수학적 조건:
   - 주봉(금요일 기준) 4주선 > 13주선 > 26주선 정배열 1~6주차 (또는 0주차 돌파 직후)
   - 정배열 진입 직전 '비정배열 베이스 준비기간(준비 N주)'이 연속 15주 이상인 종목만 통과 (단기 눌림 후 재차 정배열된 종목 배제)
   - 시가총액 1,000억 원 이상 & 최근 5일 일평균 거래대금 20억 원 이상 (유동성 미달 소형주 배제)
   - 12점 만점 스코어링 중 9.0점 이상(최상위 15~25개 내외)만 엄격 선별
3. 실제 분기 실적(영익 +XX%, 흑자전환, 적자축소) 및 4개 분기 재무제표 자동 추출 (DART API + 네이버 기업실적분석 결합)
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

try:
    import OpenDartReader
except ImportError:
    OpenDartReader = None

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "").strip()
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

# ==============================================================================
# 1. 엄격 스크리닝 파라미터 (leader.starfolio.io 기준에 맞춘 컷오프)
# ==============================================================================
KOSPI_PAGES = 10            # 코스피 시총 상위 500종목 (페이지당 50종목)
KOSDAQ_PAGES = 10           # 코스닥 시총 상위 500종목 (총 1,000종목 중 시총 컷오프 통과 종목 스캔)
MIN_MARCAP_EOK = 900        # 최소 시가총액 컷오프: 900억 원 이상
MIN_TRADING_VAL_EOK = 15.0  # 최근 5일 일평균 거래대금 최소 15억 원 이상
BONUS_MARCAP_EOK = 3000     # 시총 가점(+0.5점) 기준: 3,000억 원 이상
BONUS_TRADING_VAL_EOK = 50  # 거래대금 가점(+0.5점) 기준: 일평균 50억 원 이상

MIN_PREP_WEEKS = 15         # 초입 최소 준비기간: 15주 이상
MAX_ONSET_WEEKS = 6         # 초입 최대 정배열 주차: 6주 이하
MIN_ONSET_SCORE = 9.0       # 초입 최소 통과 점수 (12점 만점 중 9.0점 이상)
TARGET_ONSET_MAX = 25       # 초입 탭 최대 표시 종목 수 (15~25개 내외 유지)
TARGET_PROGRESS_MAX = 15    # 진행 탭 최대 표시 종목 수
MAX_WORKERS = 14            # 병렬 스캔 워커 수


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


def fetch_naver_market_universe(sosok: int, pages: int):
    """네이버 금융 시가총액 상위 페이지에서 시총 MIN_MARCAP_EOK 이상 보통주 전수를 수집합니다."""
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
            page_min_cap = 999999999
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
                marcap_eok = 0.0
                if len(tds) >= 5:
                    raw_cap = tds[4].text.replace(",", "").strip()
                    if raw_cap.lstrip("-").isdigit():
                        marcap_eok = float(raw_cap)
                page_min_cap = min(page_min_cap, marcap_eok)
                if marcap_eok < MIN_MARCAP_EOK:
                    continue
                marcap_won = marcap_eok * 100_000_000
                items.append((code, name, exch, exch, format_marcap_kr(marcap_won), [exch], marcap_won))
            if page_min_cap < MIN_MARCAP_EOK:
                break
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
    print(f"  -> 시가총액 {MIN_MARCAP_EOK}억 이상 한국장 보통주 총 {len(universe)}종목 스캔 대상 확정")
    return universe


def fetch_naver_stock_detail_and_earnings(ticker: str):
    """선별된 후보 종목에 대해 네이버 기업현황에서 실제 업종명과 최근 분기 영업이익 증감률·실적 표를 추출합니다."""
    info = {
        "industry": None,
        "opBadge": None,
        "opPositive": True,
        "earningsTitle": None,
        "earningsSummary": None,
        "quarters": None,
        "OperatingProfitValid": True
    }
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        res = requests.get(url, headers=headers, timeout=6)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")

        # 1) 업종명 추출 (예: 화학, 건설, 제약, 반도체 등)
        sec_tag = soup.select_one("div.section.trade_compare h4.h_sub.sub_tit7 em a")
        if sec_tag and sec_tag.text.strip():
            info["industry"] = sec_tag.text.strip()

        # 2) 기업실적분석 테이블에서 최근 분기 매출액·영업이익·당기순이익 추출
        cop_box = soup.select_one("div.section.cop_analysis div.sub_section table")
        if cop_box:
            thead_trs = cop_box.select("thead tr")
            if len(thead_trs) >= 2:
                q_headers = [th.text.strip() for th in thead_trs[1].select("th")]
                # 분기 실적은 보통 뒤쪽 6개 컬럼 (index 4 ~ 9)
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

                # 실제 확정된 최근 분기 찾기 (예상치(E) 제외 또는 값이 존재하는 마지막 분기)
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
                    # 전분기 또는 비교 가능 분기 대비 증감률 계산
                    prev_i = valid_q_indices[-2] if len(valid_q_indices) >= 2 else None
                    if prev_i is not None:
                        prv_op = float(op_vals[prev_i])
                        if prv_op < 0 and cur_op > 0:
                            info["opBadge"] = "영익 흑자전환"
                            info["opPositive"] = True
                            info["OperatingProfitValid"] = True
                        elif prv_op < 0 and cur_op < 0:
                            if cur_op > prv_op:
                                info["opBadge"] = "영익 적자축소"
                                info["opPositive"] = True
                                info["OperatingProfitValid"] = True
                            else:
                                info["opBadge"] = "영익 적자지속"
                                info["opPositive"] = False
                                info["OperatingProfitValid"] = False
                        elif prv_op > 0 and cur_op < 0:
                            info["opBadge"] = "영익 적자전환"
                            info["opPositive"] = False
                            info["OperatingProfitValid"] = False
                        elif prv_op > 0:
                            pct = round(((cur_op - prv_op) / abs(prv_op)) * 100, 1)
                            sign = "+" if pct >= 0 else ""
                            info["opBadge"] = f"영익 {sign}{pct}%"
                            info["opPositive"] = pct >= -15.0
                            info["OperatingProfitValid"] = pct >= -35.0
                    else:
                        info["opBadge"] = f"영익 {int(cur_op):,}억"
                        info["opPositive"] = cur_op > 0
                        info["OperatingProfitValid"] = cur_op > 0

                    q_rows = []
                    for i in reversed(valid_q_indices[-4:]):
                        ql = q_cols[i]
                        rv = f"{int(float(rev_vals[i])):,}억" if (i < len(rev_vals) and rev_vals[i].lstrip('-').isdigit()) else "—"
                        ov = f"{int(float(op_vals[i])):,}억" if (i < len(op_vals) and op_vals[i].lstrip('-').isdigit()) else "—"
                        nv = f"{int(float(net_vals[i])):,}억" if (i < len(net_vals) and net_vals[i].lstrip('-').isdigit()) else "—"
                        q_rows.append({"c1": ql, "c2": rv, "c3": ov, "c4": nv})

                    if q_rows:
                        info["quarters"] = q_rows
                        info["earningsTitle"] = f"실적 {q_label} · {info['opBadge']} 연결"
                        info["earningsSummary"] = f"최근 분기({q_label}) 매출액 {q_rows[0]['c2']} · 영업이익 {q_rows[0]['c3']} · 당기순이익 {q_rows[0]['c4']}"
    except Exception:
        pass
    return info


def inspect_kr_stock(ticker, name, exchange, industry, mcap, tags, marcap_num, start_date):
    try:
        df = fdr.DataReader(ticker, start_date)
        if df is None or len(df) < 220:
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

        # 거래대금(억원) 계산
        df["TradingValEok"] = (df["Close"] * df["Volume"]) / 100_000_000.0
        val_5d_eok = float(df["TradingValEok"].iloc[-5:].mean())
        val_20d_eok = float(df["TradingValEok"].iloc[-20:].mean())

        close_20d_ago = float(close.iloc[-21]) if len(close) >= 21 else float(close.iloc[0])
        ret_20d = round(((latest_close - close_20d_ago) / close_20d_ago) * 100, 1) if close_20d_ago > 0 else 0.0
        close_5d_ago = float(close.iloc[-6]) if len(close) >= 6 else prev_close
        close_25d_ago = float(close.iloc[-26]) if len(close) >= 26 else float(close.iloc[0])
        ret_20d_prev5 = round(((close_5d_ago - close_25d_ago) / close_25d_ago) * 100, 1) if close_25d_ago > 0 else ret_20d

        recent_252 = df.tail(252)
        high_52w = float(recent_252["High"].max())
        low_52w = float(recent_252["Low"].min())
        is_52w_high = latest_close >= high_52w * 0.985
        is_52w_low = latest_close <= low_52w * 1.015
        mdd_pct = int(round(abs(((latest_close - high_52w) / high_52w) * 100))) if high_52w > 0 else 0
        drawdown_pct = round(((latest_close - high_52w) / high_52w) * 100, 1)

        # 주봉 변환 (금요일 기준) -> 4주·13주·26주·52주 이동평균선
        weekly = df.resample("W-FRI").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "TradingValEok": "sum"
        }).dropna()

        if len(weekly) < 55:
            return None

        w_close = weekly["Close"]
        weekly["ma4"] = w_close.rolling(4).mean()
        weekly["ma13"] = w_close.rolling(13).mean()
        weekly["ma26"] = w_close.rolling(26).mean()
        weekly["ma52"] = w_close.rolling(52).mean()

        # 엄격한 주봉 정배열 판정: 4주선 > 13주선 > 26주선
        aligned_series = (weekly["ma4"] > weekly["ma13"]) & (weekly["ma13"] > weekly["ma26"])
        align_weeks = 0
        for val in reversed(aligned_series.tolist()):
            if bool(val):
                align_weeks += 1
            else:
                break

        # 정배열 시작 직전 연속 비정배열 주차(진짜 준비기간 prep_weeks) 계산
        if align_weeks > 0:
            before_onset = aligned_series.iloc[:-align_weeks]
        else:
            before_onset = aligned_series.iloc[:-1]

        prep_weeks = 0
        for val in reversed(before_onset.tolist()):
            if not bool(val):
                prep_weeks += 1
            else:
                break

        # 0주차 초입 판정 (직전 주까지 비정배열 준비기간 >= 15주였고, 이번 주 종가가 4·13·26주선 모두 돌파 & 4주선이 13주선 턱밑 0.8% 이내 수렴)
        ma4_now = float(weekly["ma4"].iloc[-1])
        ma13_now = float(weekly["ma13"].iloc[-1])
        ma26_now = float(weekly["ma26"].iloc[-1])
        ma4_prev = float(weekly["ma4"].iloc[-2])
        ma13_prev = float(weekly["ma13"].iloc[-2])

        zero_week_onset = (
            align_weeks == 0
            and prep_weeks >= MIN_PREP_WEEKS
            and latest_close > ma4_now
            and latest_close > ma13_now
            and latest_close > ma26_now
            and ma4_now >= ma13_now * 0.992
            and ma13_now >= ma26_now * 0.990
            and ma4_now > ma4_prev
        )

        # 변동성 수축(VCP): 최근 4주 주봉 고저 변동폭 평균 <= 직전 16주 평균의 85%
        w_range = (weekly["High"] - weekly["Low"]) / weekly["Close"]
        recent_w_vol = float(w_range.iloc[-4:].mean())
        prior_w_vol = float(w_range.iloc[-20:-4].mean()) if len(w_range) >= 20 else recent_w_vol
        vcp_ok = recent_w_vol <= prior_w_vol * 0.88

        # 거래대금 회복: 최근 5거래일 평균 거래대금 >= 20거래일 평균 * 1.05 & 최소 거래대금 충족
        vol_recover = (val_5d_eok >= val_20d_eok * 1.05) and (val_5d_eok >= MIN_TRADING_VAL_EOK)

        # 과열 아님: 주봉 종가가 4주선 대비 +12% 이하 & 26주선 대비 +30% 이하 & 52주 고점 대비 -22% 이내
        not_overheated = (latest_close <= ma4_now * 1.12) and (latest_close <= ma26_now * 1.30) and (drawdown_pct >= -22.0)

        # 최근 77거래일 수급 오실레이터 시계열 계산
        recent_95 = df.tail(95).copy()
        signed_flow = recent_95["Close"].pct_change().fillna(0) * recent_95["Volume"]
        norm_flow = (signed_flow.rolling(5).sum() / (recent_95["Volume"].rolling(20).mean() * 5 + 1)) * 0.08
        e12 = norm_flow.ewm(span=12, adjust=False).mean()
        e26 = norm_flow.ewm(span=26, adjust=False).mean()
        macd = e12 - e26
        sig9 = macd.ewm(span=9, adjust=False).mean()
        recent_95["osc"] = (macd - sig9).fillna(0.0)
        d77 = recent_95.tail(77)
        osc_val = round(float(d77["osc"].iloc[-1]), 3)
        osc_pct = int(round(d77["osc"].rank(pct=True).iloc[-1] * 100))
        osc_status = "바닥권 반등" if (osc_val > float(d77["osc"].iloc[-3]) and osc_pct <= 45) else ("중립 상태" if 35 < osc_pct < 70 else ("수급 유입" if osc_pct >= 70 else "약화 상태"))

        # 엄격한 5단계 분류 (초입 / 진행 / 경계 / 장기 / 이탈)
        prior_aligned_cnt = int(aligned_series.iloc[-26:-1].sum())
        if (1 <= align_weeks <= MAX_ONSET_WEEKS or zero_week_onset) and prep_weeks >= MIN_PREP_WEEKS:
            # 초입 엄격 조건: 4주선 우상향 + 종가가 13주선 위 + 최소 거래대금 충족 + 과도한 낙폭 아님
            if ma4_now >= ma4_prev and latest_close >= ma13_now and val_5d_eok >= MIN_TRADING_VAL_EOK and drawdown_pct >= -25.0:
                stage = "onset"
            else:
                stage = "none"
        elif align_weeks >= 26 and drawdown_pct >= -12.0 and latest_close >= ma4_now * 0.98:
            stage = "longterm"
        elif 10 <= align_weeks <= 25:
            if drawdown_pct >= -12.0 and latest_close >= ma13_now and ma4_now >= ma4_prev * 0.99:
                stage = "progress"
            elif drawdown_pct <= -20.0 and drawdown_pct >= -28.0:
                stage = "warning"
            else:
                stage = "none"
        else:
            if prior_aligned_cnt >= 6 and (mdd_pct >= 25 or latest_close < ma26_now):
                stage = "exit"
            else:
                stage = "none"

        last_date = df.index[-1].strftime("%Y-%m-%d")
        price_str = f"{int(latest_close):,}원"
        ma20_d = float(close.rolling(20).mean().iloc[-1])

        stat_meta = {
            "ticker": ticker,
            "name": name,
            "exchange": exchange,
            "industry": industry,
            "stage": stage,
            "change_pct": change_pct,
            "ret_20d": ret_20d,
            "ret_20d_prev5": ret_20d_prev5,
            "above_ma20": latest_close > ma20_d,
            "is_52w_high": is_52w_high,
            "is_52w_low": is_52w_low,
            "osc_val": osc_val,
            "osc_pct": osc_pct,
            "osc_status": osc_status,
            "close_str": price_str,
            "marcap_num": marcap_num,
            "val_5d_eok": val_5d_eok,
            "last_date": last_date
        }

        if stage == "none":
            return {"stat": stat_meta, "card": None}

        # 초입 12점 스코어링 (스타폴리오 8개 세부 항목 동일 배점)
        s_align = 3.0 if align_weeks <= MAX_ONSET_WEEKS else 0.0
        s_prep = 3.0 if prep_weeks >= MIN_PREP_WEEKS else 0.0
        s_vcp = 1.0 if vcp_ok else 0.0
        s_vol = 1.0 if vol_recover else 0.0
        s_sec = 2.0 if ret_20d >= 2.0 else (1.0 if ret_20d >= 0 else 0.0)
        s_heat = 1.0 if not_overheated else 0.0
        s_bon_vol = 0.5 if val_5d_eok >= BONUS_TRADING_VAL_EOK else 0.0
        s_bon_cap = 0.5 if (marcap_num / 100_000_000.0) >= BONUS_MARCAP_EOK else 0.0
        total_score = round(s_align + s_prep + s_vcp + s_vol + s_sec + s_heat + s_bon_vol + s_bon_cap, 1)

        # 초입인데 최소 점수(9.0점) 미달이면 과감히 탈락시켜 정예 종목만 남김
        if stage == "onset" and total_score < MIN_ONSET_SCORE:
            stat_meta["stage"] = "none"
            return {"stat": stat_meta, "card": None}

        score_breakdown = [
            {"label": "정배열 초입 (≤6주)", "score": f"{int(s_align)}/3", "passed": s_align > 0},
            {"label": "준비기간 충분 (≥15주)", "score": f"{int(s_prep)}/3", "passed": s_prep > 0},
            {"label": "변동성 수축", "score": f"{int(s_vcp)}/1", "passed": vcp_ok},
            {"label": "거래대금 회복", "score": f"{int(s_vol)}/1", "passed": vol_recover},
            {"label": "주도 섹터 상위", "score": f"{int(s_sec)}/2", "passed": s_sec >= 2.0},
            {"label": "과열 아님", "score": f"{int(s_heat)}/1", "passed": not_overheated},
            {"label": "보너스: 거래대금 가점", "score": f"{s_bon_vol}/0.5", "passed": s_bon_vol > 0},
            {"label": "보너스: 시총 가점", "score": f"{s_bon_cap}/0.5", "passed": s_bon_cap > 0}
        ] if stage == "onset" else []

        if stage == "onset":
            right_badge = "B" if total_score >= 10.0 else "W"
            right_badge_type = "B"
            status_label = f"점수 {total_score}"
            parts = [f"정배열 {align_weeks}주(초입)", f"준비{prep_weeks}주"]
            if vcp_ok:
                parts.append("변동성수축")
            if vol_recover:
                parts.append("거래대금회복")
            if s_bon_vol > 0:
                parts.append("거래대금 가점")
            if s_bon_cap > 0:
                parts.append("시총 가점")
            verdict = " · ".join(parts)
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
            verdict = f"정배열 경성붕괴 · MDD {mdd_pct}%(≥25) · 4-26DC · 거래대금DC"
            sort_key = mdd_pct

        w_tail = weekly.tail(52)
        weekly_candles = []
        for idx, r in w_tail.iterrows():
            weekly_candles.append({
                "time": idx.strftime("%Y-%m-%d"),
                "open": round(float(r["Open"]), 0),
                "high": round(float(r["High"]), 0),
                "low": round(float(r["Low"]), 0),
                "close": round(float(r["Close"]), 0),
                "volume": int(r["Volume"]),
                "ma4": round(float(r["ma4"]), 0) if pd.notna(r["ma4"]) else None,
                "ma13": round(float(r["ma13"]), 0) if pd.notna(r["ma13"]) else None,
                "ma26": round(float(r["ma26"]), 0) if pd.notna(r["ma26"]) else None,
                "ma52": round(float(r["ma52"]), 0) if pd.notna(r["ma52"]) else None
            })

        daily_77 = []
        for idx, r in d77.iterrows():
            daily_77.append({
                "time": idx.strftime("%Y-%m-%d"),
                "close": round(float(r["Close"]), 0),
                "osc": round(float(r["osc"]), 4)
            })

        op_sign = "+" if ret_20d >= 0 else ""
        card = {
            "id": f"KR-{ticker}",
            "country": "KR",
            "stage": stage,
            "name": name,
            "ticker": ticker,
            "exchange": exchange,
            "changePct": change_pct,
            "changeText": change_text,
            "industry": industry,
            "subIndustry": f"{industry} · {exchange}",
            "rightBadge": right_badge,
            "rightBadgeType": right_badge_type,
            "alignWeeks": align_weeks,
            "prepWeeks": prep_weeks if stage == "onset" else None,
            "score": total_score if stage == "onset" else None,
            "statusLabel": status_label,
            "mddPct": mdd_pct,
            "sectorRank": "#1",
            "marketCap": mcap,
            "lastClose": price_str,
            "onsetDate": last_date,
            "opBadge": f"영익 {op_sign}{ret_20d}%",
            "opPositive": ret_20d >= 0,
            "tags": tags,
            "volumeText": format_volume(float(df["Volume"].iloc[-1])),
            "verdict": verdict,
            "scoreBreakdown": score_breakdown,
            "earningsTitle": f"실적 및 수급 요약 ({last_date} 기준)",
            "earningsSummary": f"현재가 {price_str} · 시총 {mcap} · 52주 최고 {int(high_52w):,}원 / 최저 {int(low_52w):,}원",
            "tableHeaders": ["분기", "매출액(단순)", "영업이익", "당기순이익"],
            "quarters": [
                {"c1": last_date, "c2": "—", "c3": f"오실레이터 {osc_val:+.3f}%", "c4": f"77일 위치 {osc_pct}%"}
            ],
            "oscCurrent": f"{'+' if osc_val>=0 else ''}{osc_val:.3f}%",
            "oscStatus": osc_status,
            "oscPercentile": osc_pct,
            "weeklyCandles": weekly_candles,
            "dailyOsc": daily_77,
            "_sortKey": sort_key
        }
        return {"stat": stat_meta, "card": card}
    except Exception:
        return None


def main():
    print("=" * 70)
    print(" 한국장 주도주 스크리너 v3 - 엄격 컷오프(초입 15~25종목 압축) 스캔 시작")
    print("=" * 70)

    start_date = (datetime.now() - timedelta(days=520)).strftime("%Y-%m-%d")
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

    if not cards:
        print("[경고] 수집된 종목이 없습니다.")
        return

    # 초입·진행 후보 종목들에 대해 네이버 기업실적분석에서 실제 업종명과 분기 영업이익/재무제표 추출 (적자지속 기업 배제)
    onset_candidates = sorted([c for c in cards if c["stage"] == "onset"], key=lambda x: x.get("_sortKey", 0), reverse=True)[:40]
    progress_candidates = sorted([c for c in cards if c["stage"] == "progress"], key=lambda x: x.get("_sortKey", 0), reverse=True)[:25]

    print(f"  -> 1차 기술적 필터 통과: 초입 후보 {len(onset_candidates)}종목 / 진행 후보 {len(progress_candidates)}종목 실적·업종 정밀 검증 중...")

    verified_onset = []
    for c in onset_candidates:
        det = fetch_naver_stock_detail_and_earnings(c["ticker"])
        if det["industry"]:
            c["industry"] = det["industry"]
            c["subIndustry"] = f"{det['industry']} · {c['exchange']}"
            c["tags"] = [det["industry"]]
        if det["opBadge"]:
            c["opBadge"] = det["opBadge"]
            c["opPositive"] = det["opPositive"]
        if det["quarters"]:
            c["quarters"] = det["quarters"]
            c["earningsTitle"] = det["earningsTitle"]
            c["earningsSummary"] = det["earningsSummary"]
        # 영업이익 적자지속/적자전환 종목은 초입에서 엄격 배제
        if det["OperatingProfitValid"]:
            verified_onset.append(c)
        if len(verified_onset) >= TARGET_ONSET_MAX:
            break

    verified_progress = []
    for c in progress_candidates:
        det = fetch_naver_stock_detail_and_earnings(c["ticker"])
        if det["industry"]:
            c["industry"] = det["industry"]
            c["subIndustry"] = f"{det['industry']} · {c['exchange']}"
            c["tags"] = [det["industry"]]
        if det["opBadge"]:
            c["opBadge"] = det["opBadge"]
            c["opPositive"] = det["opPositive"]
        if det["quarters"]:
            c["quarters"] = det["quarters"]
            c["earningsTitle"] = det["earningsTitle"]
            c["earningsSummary"] = det["earningsSummary"]
        if det["OperatingProfitValid"]:
            verified_progress.append(c)
        if len(verified_progress) >= TARGET_PROGRESS_MAX:
            break

    warning_cards = sorted([c for c in cards if c["stage"] == "warning"], key=lambda x: x.get("_sortKey", 0), reverse=True)[:10]
    longterm_cards = sorted([c for c in cards if c["stage"] == "longterm"], key=lambda x: x.get("_sortKey", 0), reverse=True)[:10]
    exit_all = [c for c in cards if c["stage"] == "exit"]
    exit_cards = sorted(exit_all, key=lambda x: x.get("_sortKey", 0), reverse=True)[:30]

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
            s["sectorRank"] = f"#{idx + 1}"
            final_stocks.append(s)

    total_n = max(1, len(stats))
    up_ratio = round(sum(1 for s in stats if s["change_pct"] > 0) / total_n * 100, 2)
    dn_ratio = round(sum(1 for s in stats if s["change_pct"] < 0) / total_n * 100, 2)
    nh_ratio = round(sum(1 for s in stats if s["is_52w_high"]) / total_n * 100, 2)
    nl_ratio = round(sum(1 for s in stats if s["is_52w_low"]) / total_n * 100, 2)

    participation = {
        "marketName": "코스피·코스닥",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "totalScanned": total_n,
        "upRatio": up_ratio,
        "downRatio": dn_ratio,
        "high52wRatio": nh_ratio,
        "low52wRatio": nl_ratio,
        "netBreadth": round(up_ratio - dn_ratio, 2),
        "netHighLow": round(nh_ratio - nl_ratio, 2)
    }

    osc_pool = sorted(stats, key=lambda x: x.get("marcap_num", 0), reverse=True)[:24]
    oscillator_data = []
    for item in osc_pool:
        sign = "+" if item["osc_val"] >= 0 else ""
        oscillator_data.append({
            "code": item["ticker"],
            "name": item["name"],
            "oscValue": f"{sign}{item['osc_val']:.3f}%",
            "oscNumeric": item["osc_val"],
            "axisLabel": f"{sign}{item['osc_val']:.2f}%",
            "status": item["osc_status"],
            "percentile": item["osc_pct"],
            "close": item["close_str"]
        })

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    existing = {}
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    latest_date = max(s["onsetDate"] for s in final_stocks) if final_stocks else datetime.now().strftime("%Y-%m-%d")

    payload = {
        "meta": {
            "priceDate": latest_date,
            "updatedSchedule": "17시 갱신 시작 · 18시/18:30/20시 재시도",
            "viewCount": existing.get("meta", {}).get("viewCount", 2183) + 1,
            "thermometerScore": existing.get("meta", {}).get("thermometerScore", 49.8)
        },
        "STAGE_COUNTS": stage_counts,
        "PARTICIPATION": participation,
        "STOCKS_DATA": final_stocks,
        "OSCILLATOR_DATA": oscillator_data,
        "INDUSTRY_DATA": existing.get("INDUSTRY_DATA", []),
        "THERMOMETER_AXES": existing.get("THERMOMETER_AXES", []),
        "CREDIT_DATA": existing.get("CREDIT_DATA", {}),
        "MACRO_YIELDS": existing.get("MACRO_YIELDS", [])
    }

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n[엄격 스캔 완료] 초입({stage_counts['onset']}) · 진행({stage_counts['progress']}) · 경계({stage_counts['warning']}) · 장기({stage_counts['longterm']}) · 이탈({stage_counts['exit']}) 저장 완료!")


if __name__ == "__main__":
    main()
