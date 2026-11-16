"""
STARFOLIO 스타일 주도주 스크리너 - 업그레이드 버전:
코스피·코스닥 시가총액 & 거래대금 상위 전 종목 자동 스캔 + 미국 S&P500 주도주 스캔
실행 방법: python update_data.py
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
import FinanceDataReader as fdr

# ==============================================================================
# 스캔 설정 (원하는 규모로 조절 가능)
# ==============================================================================
KR_TOP_MARCAP = 450    # 코스피·코스닥 시가총액 상위 N개
KR_TOP_AMOUNT = 450    # 코스피·코스닥 거래대금 상위 N개 (중복 제거 후 ~230종목 스캔)
US_SCAN_LIMIT = 200     # S&P 500 상위 N개 + 핵심 테마주
MAX_WORKERS = 8        # 병렬 수집 스레드 수 (GitHub Actions 속도 최적화)
MAX_CARDS_PER_STAGE = 30  # 단계별 웹 화면에 표시할 최대 상위 종목 수

# S&P500 외 추가 감시할 미국 핵심 주도주 (AI·전력·바이오·보안)
US_EXTRA_WATCHLIST = [
    ("NVDA", "NVIDIA Corp.", "NASDAQ", "반도체", "AI대장주", ["AI칩", "데이터센터"]),
    ("PLTR", "Palantir Technologies", "NASDAQ", "소프트웨어", "AI소프트웨어", ["국방AI", "AIP"]),
    ("GEV", "GE Vernova Inc.", "NYSE", "에너지/전력", "전력인프라", ["가스터빈", "그리드"]),
    ("VRT", "Vertiv Holdings", "NYSE", "인프라", "데이터센터", ["액체냉각", "전력"]),
    ("ROIV", "Roivant Sciences", "NASDAQ", "헬스케어", "Biotech", ["바이오"]),
    ("MEDP", "Medpace Holdings", "NASDAQ", "헬스케어", "CRO", ["임상수탁"]),
    ("HALO", "Halozyme Therapeutics", "NASDAQ", "헬스케어", "Biotech", ["SC제형"]),
    ("OKTA", "Okta Inc.", "NASDAQ", "IT", "사이버보안", ["보안"])
]

# KRX 리스팅 장애 시 예비 목록
FALLBACK_KR = [
    ("005930", "삼성전자", "KOSPI", "반도체", 0, ["반도체"]),
    ("000660", "SK하이닉스", "KOSPI", "반도체", 0, ["HBM", "AI반도체"]),
    ("267260", "HD현대일렉트릭", "KOSPI", "전기전자", 0, ["변압기", "AI전력"]),
    ("012450", "한화에어로스페이스", "KOSPI", "방산", 0, ["K-방산"]),
    ("251970", "펌텍코리아", "KOSDAQ", "화학", 0, ["화장품"]),
    ("028050", "삼성E&A", "KOSPI", "건설", 0, ["엔지니어링"]),
    ("086450", "동국제약", "KOSDAQ", "제약", 0, ["바이오"]),
    ("093320", "케이아이엔엑스", "KOSDAQ", "IT", 0, ["클라우드"]),
    ("089860", "롯데렌탈", "KOSPI", "서비스", 0, ["밸류업"]),
    ("159010", "아스플로", "KOSDAQ", "기계", 0, ["반도체장비"]),
    ("003230", "삼양식품", "KOSPI", "음식료", 0, ["K-푸드"]),
    ("004490", "세방전지", "KOSPI", "자동차부품", 0, ["밸류업"])
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


def compute_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def build_kr_universe():
    """코스피·코스닥 전체 상장종목 중 스팩/우선주를 제외하고 시총·거래대금 상위 종목을 추출합니다."""
    try:
        df = fdr.StockListing("KRX")
        if df is None or df.empty:
            raise ValueError("Empty KRX listing")

        code_col = "Code" if "Code" in df.columns else "Symbol"
        name_col = "Name" if "Name" in df.columns else df.columns[1]
        mkt_col = "Market" if "Market" in df.columns else None
        cap_col = "Marcap" if "Marcap" in df.columns else ("MarketCap" if "MarketCap" in df.columns else None)
        amt_col = "Amount" if "Amount" in df.columns else ("Volume" if "Volume" in df.columns else None)
        sec_col = "Sector" if "Sector" in df.columns else ("Dept" if "Dept" in df.columns else None)

        if mkt_col:
            df = df[df[mkt_col].isin(["KOSPI", "KOSDAQ"])].copy()

        # 스팩, 우선주(끝자리 0이 아닌 보통주 외 종목), 리츠 등 잡음 필터링
        df[code_col] = df[code_col].astype(str).str.zfill(6)
        df = df[df[code_col].str.endswith("0")]
        df = df[~df[name_col].astype(str).str.contains("스팩|ETN|ETF|리츠", na=False)]

        top_frames = []
        if cap_col:
            df[cap_col] = pd.to_numeric(df[cap_col], errors="coerce").fillna(0)
            top_frames.append(df.sort_values(cap_col, ascending=False).head(KR_TOP_MARCAP))
        if amt_col:
            df[amt_col] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)
            top_frames.append(df.sort_values(amt_col, ascending=False).head(KR_TOP_AMOUNT))

        merged = pd.concat(top_frames).drop_duplicates(subset=[code_col]) if top_frames else df.head(200)
        universe = []
        for _, row in merged.iterrows():
            code = str(row[code_col])
            name = str(row[name_col])
            exch = str(row[mkt_col]) if mkt_col else "KRX"
            sec = str(row[sec_col]) if (sec_col and pd.notna(row[sec_col]) and str(row[sec_col]).strip()) else exch
            marcap_str = format_marcap_kr(row[cap_col]) if cap_col else "—"
            marcap_num = float(row[cap_col]) if cap_col else 0.0
            universe.append((code, name, exch, sec, marcap_str, [sec], marcap_num))
        print(f"  -> KRX 시총/거래대금 상위 {len(universe)}개 종목 추출 완료")
        return universe
    except Exception as e:
        print(f"  [경고] KRX 전체 리스팅 실패({e}), 예비 12종목으로 진행합니다.")
        return [(c, n, ex, sec, "—", tags, 0.0) for c, n, ex, sec, _, tags in FALLBACK_KR]


def build_us_universe():
    """S&P 500 상위 종목과 핵심 미국 주도주 리스트를 결합합니다."""
    seen = set()
    universe = []
    for t, n, ex, sec, mcap, tags in US_EXTRA_WATCHLIST:
        seen.add(t)
        universe.append((t, n, ex, sec, mcap, tags, 0.0))

    try:
        sp = fdr.StockListing("S&P500")
        if sp is not None and not sp.empty:
            sym_col = "Symbol" if "Symbol" in sp.columns else sp.columns[0]
            name_col = "Name" if "Name" in sp.columns else sp.columns[1]
            sec_col = "Sector" if "Sector" in sp.columns else None
            for _, row in sp.head(US_SCAN_LIMIT).iterrows():
                sym = str(row[sym_col]).replace(".", "-")
                if sym in seen:
                    continue
                seen.add(sym)
                name = str(row[name_col])
                sec = str(row[sec_col]) if (sec_col and pd.notna(row[sec_col])) else "US Equity"
                universe.append((sym, name, "US", sec, "S&P500", [sec], 0.0))
        print(f"  -> 미국(S&P500 + 핵심주) {len(universe)}개 종목 추출 완료")
    except Exception as e:
        print(f"  [경고] S&P500 리스팅 실패({e}), 핵심 8종목만 스캔합니다.")
    return universe


def analyze_stock(ticker, name, exchange, industry, mcap, tags, marcap_num, country, start_date):
    try:
        df = fdr.DataReader(ticker, start_date)
        if df is None or len(df) < 130:
            return None

        df = df.dropna(subset=["Close"]).copy()
        close = df["Close"]
        latest_close = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        if prev_close <= 0:
            return None

        change_pct = round(((latest_close - prev_close) / prev_close) * 100, 1)
        change_arrow = "▲" if change_pct > 0 else ("▼" if change_pct < 0 else "-")
        change_text = f"{change_arrow}{abs(change_pct):.1f}%"

        # 일봉 20일·120일 이평선 & RSI(14)
        ma20_d = float(close.rolling(20).mean().iloc[-1])
        ma120_d = float(close.rolling(120).mean().iloc[-1])
        rsi14 = round(compute_rsi(close, 14), 1)

        # 주봉 변환 -> 4주·13주·26주 이평선
        weekly = df.resample("W-FRI").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum"
        }).dropna()

        if len(weekly) < 28:
            return None

        w_close = weekly["Close"]
        w_ma4 = w_close.rolling(4).mean()
        w_ma13 = w_close.rolling(13).mean()
        w_ma26 = w_close.rolling(26).mean()

        aligned_series = (w_ma4 > w_ma13) & (w_ma13 > w_ma26)
        align_weeks = 0
        for val in reversed(aligned_series.tolist()):
            if bool(val):
                align_weeks += 1
            else:
                break

        high_52w = float(df["High"].max())
        drawdown_pct = round(((latest_close - high_52w) / high_52w) * 100, 1)

        # 변동성 수축(VCP) 및 거래대금 회복
        daily_range = (df["High"] - df["Low"]) / df["Close"]
        recent_vol = float(daily_range.iloc[-20:].mean())
        prior_vol = float(daily_range.iloc[-80:-20].mean()) if len(daily_range) >= 80 else recent_vol
        vcp_ok = recent_vol <= prior_vol * 0.90

        vol_5d = float(df["Volume"].iloc[-5:].mean())
        vol_20d = float(df["Volume"].iloc[-20:].mean())
        vol_recover = vol_5d >= vol_20d * 1.05
        not_overheated = (rsi14 < 72.0) and (latest_close < ma20_d * 1.15)

        # 추세 단계 판정 (전수 스캔 시 비정배열 잡주 제외하고 의미 있는 종목만 선별)
        if align_weeks == 0:
            if latest_close > ma20_d and ma20_d > ma120_d and vol_recover:
                stage = "onset"
            else:
                return None  # 완전히 역배열/하락추세인 종목은 스캔 결과에서 제외
        elif 1 <= align_weeks <= 6:
            stage = "onset"
        else:
            stage = "warning" if (drawdown_pct <= -15.0 or latest_close < float(w_ma4.iloc[-1])) else "progress"

        # 12점 스코어링
        prep_weeks = max(15, 52 - align_weeks)
        s_align = 3.0 if align_weeks <= 6 else 0.0
        s_prep = 3.0 if prep_weeks >= 15 else 0.0
        s_vcp = 1.0 if vcp_ok else 0.0
        s_vol = 1.0 if vol_recover else 0.0
        s_sector = 2.0 if (latest_close > ma20_d and ma20_d > ma120_d) else 1.0
        s_heat = 1.0 if not_overheated else 0.0
        s_bonus = 1.0 if (marcap_num >= 300_000_000_000 or country == "US") else 0.5
        total_score = round(s_align + s_prep + s_vcp + s_vol + s_sector + s_heat + s_bonus, 1)

        score_breakdown = [
            {"label": "정배열 초입 (≤6주)", "score": f"{int(s_align)}/3", "passed": s_align > 0},
            {"label": "준비기간 충분 (≥15주)", "score": f"{int(s_prep)}/3", "passed": s_prep > 0},
            {"label": "변동성 수축 (VCP)", "score": f"{int(s_vcp)}/1", "passed": vcp_ok},
            {"label": "거래량 회복 (5일≥20일)", "score": f"{int(s_vol)}/1", "passed": vol_recover},
            {"label": "20일·120일선 정배열", "score": f"{int(s_sector)}/2", "passed": s_sector >= 2.0},
            {"label": "과열 아님 (RSI<72)", "score": f"{int(s_heat)}/1", "passed": not_overheated},
            {"label": "보너스: 시총·유동성", "score": f"{s_bonus}/1", "passed": s_bonus >= 1.0}
        ] if stage == "onset" else []

        if stage == "onset":
            right_badge = "B" if total_score >= 10 else "W"
            right_badge_type = "B"
            status_label = f"점수 {total_score}"
            verdict = f"정배열 {align_weeks}주(초입) · 20일선 {'>' if ma20_d > ma120_d else '<'} 120일선 · RSI {rsi14} · {'변동성수축' if vcp_ok else '거래회복'}"
        elif stage == "progress":
            right_badge = "확산"
            right_badge_type = "DIFF"
            status_label = "신고가" if drawdown_pct >= -3.0 else f"고점 {drawdown_pct:.0f}%"
            verdict = f"확산 · 정배열 {align_weeks}주 · {status_label} · RSI {rsi14} — 추세 진행 중"
        else:
            right_badge = "경계"
            right_badge_type = "WARN"
            status_label = f"고점 {drawdown_pct:.0f}%"
            verdict = f"경계 · 정배열 {align_weeks}주 · 고점 대비 {drawdown_pct:.1f}% 조정 — 지지선 점검"

        sampled = df.iloc[::7].tail(17)
        if sampled.index[-1] != df.index[-1]:
            sampled = pd.concat([sampled, df.iloc[[-1]]])
        series = [{"d": idx.strftime("%m-%d"), "p": round(float(row["Close"]), 2)} for idx, row in sampled.iterrows()]

        price_str = f"{int(latest_close):,}원" if country == "KR" else f"${latest_close:,.2f}"
        ma20_str = f"{int(ma20_d):,}원" if country == "KR" else f"${ma20_d:,.2f}"
        ma120_str = f"{int(ma120_d):,}원" if country == "KR" else f"${ma120_d:,.2f}"
        high_str = f"{int(high_52w):,}원" if country == "KR" else f"${high_52w:,.2f}"
        last_date = df.index[-1].strftime("%Y-%m-%d")

        return {
            "id": f"{country}-{ticker}",
            "country": country,
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
            "sectorRank": "#자동스캔",
            "marketCap": mcap,
            "lastClose": price_str,
            "onsetDate": last_date,
            "opBadge": f"RSI {rsi14}",
            "opPositive": rsi14 >= 50,
            "tags": tags,
            "volumeText": format_volume(float(df["Volume"].iloc[-1])),
            "verdict": verdict,
            "scoreBreakdown": score_breakdown,
            "earningsTitle": f"일봉·주봉 이동평균선 자동 스캔 ({last_date} 기준)",
            "earningsSummary": f"현재가 {price_str} · 20일선 {ma20_str} · 120일선 {ma120_str} · 기간 고점 대비 {drawdown_pct:.1f}%",
            "tableHeaders": ["기준일", "단기선 (20일)", "장기선 (120일)", "기간 고점 / RSI"],
            "quarters": [
                {"c1": last_date, "c2": ma20_str, "c3": ma120_str, "c4": f"{high_str} / RSI {rsi14}"}
            ],
            "series": series,
            "_sortKey": total_score if stage == "onset" else (100 + drawdown_pct)
        }
    except Exception:
        return None


def scan_universe(universe, country, start_date):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(analyze_stock, t, n, ex, sec, mcap, tags, mnum, country, start_date)
            for (t, n, ex, sec, mcap, tags, mnum) in universe
        ]
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                results.append(res)
    return results


def main():
    print("=" * 60)
    print(" 주도주 스크리너 - 전 종목(코스피·코스닥·S&P500) 자동 스캔 시작")
    print("=" * 60)

    start_date = (datetime.now() - timedelta(days=270)).strftime("%Y-%m-%d")

    print("\n[1/2] 한국(KRX) 시총·거래대금 상위 유니버스 구성 및 스캔 중...")
    kr_univ = build_kr_universe()
    kr_results = scan_universe(kr_univ, "KR", start_date)

    print("\n[2/2] 미국(S&P500 + 핵심주) 유니버스 구성 및 스캔 중...")
    us_univ = build_us_universe()
    us_results = scan_universe(us_univ, "US", start_date)

    all_raw = kr_results + us_results
    if not all_raw:
        print("\n[경고] 수집된 종목이 없어 기존 data.json을 유지합니다.")
        return

    # 단계별 실제 전체 건수 집계 후, 화면에는 단계별 상위 MAX_CARDS_PER_STAGE개만 정렬해 저장
    stage_counts = {
        "KR": {"onset": 0, "progress": 0, "warning": 0},
        "US": {"onset": 0, "progress": 0, "warning": 0}
    }
    final_stocks = []
    for c in ["KR", "US"]:
        for st in ["onset", "progress", "warning"]:
            subset = [s for s in all_raw if s["country"] == c and s["stage"] == st]
            stage_counts[c][st] = len(subset)
            subset.sort(key=lambda x: x.get("_sortKey", 0), reverse=True)
            for s in subset[:MAX_CARDS_PER_STAGE]:
                s.pop("_sortKey", None)
                final_stocks.append(s)

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    existing = {}
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    latest_price_date = max(s["onsetDate"] for s in final_stocks)

    updated_payload = {
        "meta": {
            "priceDate": latest_price_date,
            "updatedSchedule": "17시 갱신 시작 · 18시/18:30/20시 재시도",
            "viewCount": existing.get("meta", {}).get("viewCount", 2183) + 1
        },
        "TAB_META": {
            "KR": {
                "onset": {"label": "초입", "realTotal": stage_counts["KR"]["onset"], "desc": f"코스피·코스닥 시총/거래대금 상위 {len(kr_univ)}종목 중 정배열 초입 포착"},
                "progress": {"label": "진행", "realTotal": stage_counts["KR"]["progress"], "desc": "정배열 7주 이상 확산 · 고점 대비 -15% 이내 진행 종목"},
                "warning": {"label": "경계", "realTotal": stage_counts["KR"]["warning"], "desc": "고점 대비 -15% 초과 조정 또는 4주선 이탈 경계 종목"}
            },
            "US": {
                "onset": {"label": "초입", "realTotal": stage_counts["US"]["onset"], "desc": f"미국 S&P500·핵심주 {len(us_univ)}종목 중 정배열 초입 포착"},
                "progress": {"label": "진행", "realTotal": stage_counts["US"]["progress"], "desc": "미국 시장 정배열 확산 진행 종목"},
                "warning": {"label": "경계", "realTotal": stage_counts["US"]["warning"], "desc": "미국 시장 추세 경계 종목"}
            }
        },
        "STOCKS_DATA": final_stocks,
        "OSCILLATOR_DATA": existing.get("OSCILLATOR_DATA", []),
        "INDUSTRY_DATA": existing.get("INDUSTRY_DATA", []),
        "THERMOMETER_AXES": existing.get("THERMOMETER_AXES", []),
        "MACRO_YIELDS": existing.get("MACRO_YIELDS", []),
        "SIMULATOR_RULES": existing.get("SIMULATOR_RULES", [])
    }

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(updated_payload, f, ensure_ascii=False, indent=2)

    print(f"\n[완료] 한국(초입 {stage_counts['KR']['onset']} / 진행 {stage_counts['KR']['progress']} / 경계 {stage_counts['KR']['warning']}) · "
          f"미국(초입 {stage_counts['US']['onset']} / 진행 {stage_counts['US']['progress']} / 경계 {stage_counts['US']['warning']}) 저장 완료! (기준일: {latest_price_date})")


if __name__ == "__main__":
    main()
