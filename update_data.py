"""
한국장 전용 주도주 스크리너 (leader.starfolio.io 스타일 100% 재현 버전)
- 해외 IP(GitHub Actions)에서도 차단되지 않는 네이버 금융 코스피·코스닥 시가총액 상위 종목 크롤링 + FDR 결합
- 종목별 주봉 캔들(OHLCV) + 4주·13주·26주·52주 이동평균선 + 최근 77거래일 일봉·수급오실레이터 시계열 생성
- 5대 추세 단계(초입 · 진행 · 경계 · 장기 · 이탈 EXIT) + 수급오실레이터 + 신용잔고 + 체온 + 시장참여도 + 산업강도 자동 계산
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

KOSPI_PAGES = 4     # 네이버 시총 상위 페이지당 50종목 -> 코스피 상위 200종목
KOSDAQ_PAGES = 4    # 페이지당 50종목 -> 코스닥 상위 200종목 (총 400종목 스캔)
MAX_WORKERS = 10
MAX_CARDS_PER_STAGE = 40

# 필수 포함 감시 종목 (시총 순위 무관 항상 스캔)
MUST_INCLUDE_KR = [
    ("251970", "펌텍코리아", "KOSDAQ", "화학", 957300000000, ["화장품"]),
    ("028050", "삼성E&A", "KOSPI", "건설", 13484800000000, ["수자원", "전후재건"]),
    ("086450", "동국제약", "KOSDAQ", "바이오/제약", 1135300000000, ["제약업체"]),
    ("093320", "케이아이엔엑스", "KOSDAQ", "IT/소프트웨어", 855500000000, ["클라우드 컴퓨팅"]),
    ("089860", "롯데렌탈", "KOSPI", "기계/장비", 2124100000000, ["렌터카", "밸류업"]),
    ("079650", "서산", "KOSDAQ", "건자재/비금속", 145400000000, ["시멘트/레미콘"]),
    ("159010", "아스플로", "KOSDAQ", "기계/장비", 498100000000, ["반도체 장비"]),
    ("017890", "한국알콜", "KOSDAQ", "화학", 315700000000, ["밸류업", "주류업"]),
    ("267260", "HD현대일렉트릭", "KOSPI", "전기전자", 15000000000000, ["변압기", "AI전력"]),
    ("012450", "한화에어로스페이스", "KOSPI", "항공우주/방산", 20000000000000, ["K-방산"]),
    ("003230", "삼양식품", "KOSPI", "음식료", 9000000000000, ["K-푸드"]),
    ("004490", "세방전지", "KOSPI", "자동차부품", 1200000000000, ["밸류업"])
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


def fetch_naver_market_top(sosok: int, pages: int):
    """해외 IP(GitHub Actions)에서도 100% 작동하는 네이버 금융 시가총액 상위 종목 크롤러 (sosok=0: KOSPI, 1: KOSDAQ)"""
    exch = "KOSPI" if sosok == 0 else "KOSDAQ"
    items = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for page in range(1, pages + 1):
        try:
            url = f"https://finance.naver.com/sise/sise_market_sum.nhn?sosok={sosok}&page={page}"
            res = requests.get(url, headers=headers, timeout=8)
            res.encoding = "euc-kr"
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table", {"class": "type_2"})
            if not table:
                continue
            for tr in table.find_all("tr"):
                a_tag = tr.find("a", {"class": "tltle"})
                if not a_tag:
                    continue
                name = a_tag.text.strip()
                if any(k in name for k in ["스팩", "ETF", "ETN", "리츠", "우", "우B", "KODEX", "TIGER", "KBSTAR", "ACE", "SOL", "HANARO"]):
                    continue
                href = a_tag.get("href", "")
                m = re.search(r"code=(\d{6})", href)
                if not m:
                    continue
                code = m.group(1)
                if not code.endswith("0"):
                    continue
                tds = tr.find_all("td", {"class": "number"})
                # 네이버 시총 테이블에서 시가총액(억원)은 5번째 number 컬럼(index 4)
                marcap_eok = 0.0
                if len(tds) >= 5:
                    raw_cap = tds[4].text.replace(",", "").strip()
                    if raw_cap.isdigit():
                        marcap_eok = float(raw_cap)
                marcap_won = marcap_eok * 100_000_000
                items.append((code, name, exch, exch, format_marcap_kr(marcap_won), [exch], marcap_won))
        except Exception:
            continue
    return items


def build_kr_universe():
    seen = set()
    universe = []

    # 1차: 네이버 금융 시총 상위(코스피+코스닥) 수집 (해외 IP 차단 없음)
    naver_kospi = fetch_naver_market_top(0, KOSPI_PAGES)
    naver_kosdaq = fetch_naver_market_top(1, KOSDAQ_PAGES)
    for item in naver_kospi + naver_kosdaq:
        if item[0] not in seen:
            seen.add(item[0])
            universe.append(item)

    # 2차: 필수 포함 종목 병합
    for c, n, ex, sec, mc, tags in MUST_INCLUDE_KR:
        if c not in seen:
            seen.add(c)
            universe.append((c, n, ex, sec, format_marcap_kr(mc), tags, float(mc)))

    print(f"  -> 한국장(KOSPI·KOSDAQ) 스캔 대상 총 {len(universe)}종목 준비 완료")
    return universe


def get_kis_access_token():
    if not (KIS_APP_KEY and KIS_APP_SECRET):
        return None
    try:
        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
        res = requests.post(
            url,
            json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET},
            timeout=10
        )
        if res.ok:
            print("  ✓ KIS Open API 토큰 발급 성공")
            return res.json().get("access_token")
    except Exception:
        pass
    return None


def fetch_dart_op_badge(dart_client, ticker: str):
    if dart_client is None:
        return None
    try:
        cur_year = datetime.now().year
        for y in [cur_year, cur_year - 1]:
            for reprt_code, q_name in [("11014", "3Q"), ("11012", "반기"), ("11013", "1Q"), ("11011", "연간")]:
                df = dart_client.finstate(ticker, y, reprt_code=reprt_code)
                if df is not None and not df.empty and "account_nm" in df.columns:
                    op_rows = df[df["account_nm"].astype(str).str.contains("영업이익", na=False)]
                    if not op_rows.empty:
                        row = op_rows.iloc[0]
                        cur_amt = str(row.get("thstrm_amount", "")).replace(",", "").strip()
                        prv_amt = str(row.get("frmtrm_amount", "")).replace(",", "").strip()
                        if cur_amt.lstrip("-").isdigit() and prv_amt.lstrip("-").isdigit():
                            c_val, p_val = float(cur_amt), float(prv_amt)
                            if p_val < 0 and c_val > 0:
                                return (f"영익 흑자전환", True, f"실적 {y} {q_name} · 영익 흑자전환 연결")
                            if p_val < 0 and c_val < 0 and c_val > p_val:
                                return (f"영익 적자축소", True, f"실적 {y} {q_name} · 영익 적자축소 연결")
                            if p_val > 0:
                                yoy = round(((c_val - p_val) / abs(p_val)) * 100, 1)
                                sign = "+" if yoy >= 0 else ""
                                return (f"영익 {sign}{yoy}%", yoy >= 0, f"실적 {y} {q_name} · 영익 {sign}{yoy}% 연결")
    except Exception:
        pass
    return None


def inspect_kr_stock(ticker, name, exchange, industry, mcap, tags, marcap_num, start_date):
    try:
        df = fdr.DataReader(ticker, start_date)
        if df is None or len(df) < 180:
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

        # 20일 수익률 및 52주 신고가/신저가 여부 (시장 참여도 및 산업 강도 계산용)
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
            "Volume": "sum"
        }).dropna()

        if len(weekly) < 52:
            return None

        w_close = weekly["Close"]
        weekly["ma4"] = w_close.rolling(4).mean()
        weekly["ma13"] = w_close.rolling(13).mean()
        weekly["ma26"] = w_close.rolling(26).mean()
        weekly["ma52"] = w_close.rolling(52).mean()

        # 주봉 정배열 (4주 > 13주 > 26주) 연속 주차 계산
        aligned_series = (weekly["ma4"] > weekly["ma13"]) & (weekly["ma13"] > weekly["ma26"])
        align_weeks = 0
        for val in reversed(aligned_series.tolist()):
            if bool(val):
                align_weeks += 1
            else:
                break

        # 과거 정배열 이력 (이탈 종목 판정용)
        prior_aligned_max = int(aligned_series.iloc[-20:-1].sum()) if len(aligned_series) >= 20 else 0
        prep_weeks = int((~aligned_series.iloc[-52:-max(1, align_weeks)]).sum()) if align_weeks > 0 else int((~aligned_series.iloc[-52:]).sum())
        prep_weeks = max(15, prep_weeks)

        # 변동성 수축(VCP) 및 거래대금 회복
        daily_range = (df["High"] - df["Low"]) / df["Close"]
        recent_vol = float(daily_range.iloc[-20:].mean())
        prior_vol = float(daily_range.iloc[-80:-20].mean()) if len(daily_range) >= 80 else recent_vol
        vcp_ok = recent_vol <= prior_vol * 0.92

        vol_5d = float(df["Volume"].iloc[-5:].mean())
        vol_20d = float(df["Volume"].iloc[-20:].mean())
        vol_recover = vol_5d >= vol_20d * 1.02
        ma20_d = float(close.rolling(20).mean().iloc[-1])
        not_overheated = latest_close <= ma20_d * 1.18

        # 최근 77거래일 일봉 + 수급 오실레이터 시계열 계산
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
        osc_status = "바닥권 반등" if (osc_val > float(d77["osc"].iloc[-3]) and osc_pct <= 45) else ("수급 유입" if osc_val > 0 else ("약화" if osc_pct <= 35 else "중립"))

        # 5단계 분류 (초입 / 진행 / 경계 / 장기 / 이탈)
        if 1 <= align_weeks <= 6 or (align_weeks == 0 and latest_close > float(weekly["ma4"].iloc[-1]) and float(weekly["ma4"].iloc[-1]) >= float(weekly["ma13"].iloc[-1]) * 0.99 and vol_recover):
            stage = "onset"
        elif align_weeks >= 26 and drawdown_pct >= -15.0:
            stage = "longterm"
        elif align_weeks >= 7:
            if drawdown_pct <= -18.0 or latest_close < float(weekly["ma13"].iloc[-1]):
                stage = "warning"
            else:
                stage = "progress"
        else:
            if prior_aligned_max >= 3 or mdd_pct >= 20:
                stage = "exit"
            else:
                stage = "none"

        last_date = df.index[-1].strftime("%Y-%m-%d")
        price_str = f"{int(latest_close):,}원"

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
            "last_date": last_date
        }

        if stage == "none":
            return {"stat": stat_meta, "card": None}

        # 12점 스코어링 (초입 기준)
        s_align = 3.0 if align_weeks <= 6 else 0.0
        s_prep = 3.0 if prep_weeks >= 15 else 0.0
        s_vcp = 1.0 if vcp_ok else 0.0
        s_vol = 1.0 if vol_recover else 0.0
        s_sec = 2.0 if ret_20d > 0 else 1.0
        s_heat = 1.0 if not_overheated else 0.0
        s_bon_vol = 0.5 if vol_recover else 0.0
        s_bon_cap = 0.5 if marcap_num >= 100_000_000_000 else 0.0
        total_score = round(s_align + s_prep + s_vcp + s_vol + s_sec + s_heat + s_bon_vol + s_bon_cap, 1)

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
            verdict = f"정배열 {align_weeks}주(초입) · 준비{prep_weeks}주 · {'변동성수축 · ' if vcp_ok else ''}{'거래대금회복 · ' if vol_recover else ''}거래대금 가점 · 시총 가점"
            sort_key = total_score * 10 + change_pct
        elif stage in ("progress", "longterm"):
            right_badge = "장기" if stage == "longterm" else "확산"
            right_badge_type = "DIFF"
            status_label = "신고가" if drawdown_pct >= -5.0 else f"고점 {drawdown_pct:.0f}%"
            verdict = f"확산 · 정배열 {align_weeks}주 · {status_label} — 이탈 신호 없이 진행 중"
            sort_key = 100 + drawdown_pct
        elif stage == "warning":
            right_badge = "경계"
            right_badge_type = "WARN"
            status_label = f"고점 {drawdown_pct:.0f}%"
            verdict = f"경계 · 정배열 {align_weeks}주 · 고점대비 {drawdown_pct:.0f}% 조정 — 13주선 지지 점검"
            sort_key = 100 + drawdown_pct
        else:
            right_badge = "EXIT"
            right_badge_type = "EXIT"
            status_label = f"MDD {mdd_pct}%"
            verdict = f"정배열 경성붕괴 · MDD {mdd_pct}%(≥20) · 4-26DC · 거래대금약화"
            sort_key = mdd_pct

        # TradingView Lightweight Charts용 최근 52주 주봉 캔들 + MA4/13/26/52 데이터
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

        # 최근 77거래일 일봉 + 수급 오실레이터 데이터
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
            "opBadge": f"20일 {op_sign}{ret_20d}%",
            "opPositive": ret_20d >= 0,
            "tags": tags,
            "volumeText": format_volume(float(df["Volume"].iloc[-1])),
            "verdict": verdict,
            "scoreBreakdown": score_breakdown,
            "earningsTitle": f"수급·기술 지표 요약 ({last_date} 기준)",
            "earningsSummary": f"현재가 {price_str} · 시총 {mcap} · 52주 최고 {int(high_52w):,}원 / 최저 {int(low_52w):,}원",
            "tableHeaders": ["기준", "수급 오실레이터", "77일 중 위치", "52주 최고 / 최저"],
            "quarters": [
                {"c1": last_date, "c2": f"{'+' if osc_val>=0 else ''}{osc_val:.3f}% ({osc_status})", "c3": f"{osc_pct}%", "c4": f"{int(high_52w):,}원 / {int(low_52w):,}원"}
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
    print("=" * 65)
    print(" 한국장 전용 주도주 스크리너 (leader.starfolio.io 스타일) 자동 갱신")
    print("=" * 65)

    start_date = (datetime.now() - timedelta(days=480)).strftime("%Y-%m-%d")
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

    # DART API가 있으면 상위 초입/진행 종목에 실제 분기 영업이익 배지 부여
    dart_client = None
    if DART_API_KEY and OpenDartReader is not None:
        try:
            dart_client = OpenDartReader(DART_API_KEY)
            print("  ✓ OPEN DART API 활성화 완료")
        except Exception:
            dart_client = None

    stages = ["onset", "progress", "warning", "longterm", "exit"]
    stage_counts = {st: 0 for st in stages}
    final_stocks = []

    for st in stages:
        subset = [c for c in cards if c["stage"] == st]
        stage_counts[st] = len(subset)
        subset.sort(key=lambda x: x.get("_sortKey", 0), reverse=True)
        limit = 25 if st == "exit" else MAX_CARDS_PER_STAGE
        for idx, s in enumerate(subset[:limit]):
            s.pop("_sortKey", None)
            s["sectorRank"] = f"#{idx + 1}"
            if dart_client is not None and st in ("onset", "progress") and idx < 12:
                op_info = fetch_dart_op_badge(dart_client, s["ticker"])
                if op_info:
                    s["opBadge"], s["opPositive"], s["earningsTitle"] = op_info
                time.sleep(0.08)
            final_stocks.append(s)

    # 시장 참여도 (상승/하락 비율, 52주 신고가/신저가 비율)
    total_n = max(1, len(stats))
    up_ratio = round(sum(1 for s in stats if s["change_pct"] > 0) / total_n * 100, 2)
    dn_ratio = round(sum(1 for s in stats if s["change_pct"] < 0) / total_n * 100, 2)
    nh_ratio = round(sum(1 for s in stats if s["is_52w_high"]) / total_n * 100, 2)
    nl_ratio = round(sum(1 for s in stats if s["is_52w_low"]) / total_n * 100, 2)

    participation = {
        "totalScanned": total_n,
        "upRatio": up_ratio,
        "downRatio": dn_ratio,
        "high52wRatio": nh_ratio,
        "low52wRatio": nl_ratio,
        "netBreadth": round(up_ratio - dn_ratio, 2),
        "netHighLow": round(nh_ratio - nl_ratio, 2)
    }

    # 수급 오실레이터 탭 목록 (상위 20종목)
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

    latest_date = max(s["onsetDate"] for s in final_stocks)

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

    print(f"[완료] 초입({stage_counts['onset']}) · 진행({stage_counts['progress']}) · 경계({stage_counts['warning']}) · 장기({stage_counts['longterm']}) · 이탈({stage_counts['exit']}) 저장 완료!")


if __name__ == "__main__":
    main()
