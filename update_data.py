"""
주도주 스크리너 - 전 탭(초입/진행/경계 · 수급오실레이터 · 산업강도 · 시장체온계 · 매크로금리) 통합 자동 갱신 엔진
- 기본 모드(API 키 없음): FinanceDataReader만으로 5개 탭 전체를 매일 100% 자동 계산
- 확장 모드(GitHub Secrets에 KIS_APP_KEY, KIS_APP_SECRET, DART_API_KEY 등록 시):
  한국투자증권 실수급(외국인+기관 순매수) 및 DART 분기 영업이익 자동 결합
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd
import requests
import FinanceDataReader as fdr

try:
    import OpenDartReader
except ImportError:
    OpenDartReader = None

# ==============================================================================
# 1. 환경변수 (GitHub Secrets에 등록하면 자동 활성화, 없으면 무료 지수/시세 계산으로 대체)
# ==============================================================================
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "").strip()
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

KR_TOP_MARCAP = 150
KR_TOP_AMOUNT = 150
US_SCAN_LIMIT = 80
MAX_WORKERS = 8
MAX_CARDS_PER_STAGE = 30

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

FALLBACK_KR = [
    ("005930", "삼성전자", "KOSPI", "반도체", 400000000000000, ["반도체"]),
    ("000660", "SK하이닉스", "KOSPI", "반도체", 120000000000000, ["HBM", "AI반도체"]),
    ("267260", "HD현대일렉트릭", "KOSPI", "전기전자", 15000000000000, ["변압기", "AI전력"]),
    ("012450", "한화에어로스페이스", "KOSPI", "항공우주/방산", 20000000000000, ["K-방산"]),
    ("251970", "펌텍코리아", "KOSDAQ", "화학", 916400000000, ["화장품"]),
    ("028050", "삼성E&A", "KOSPI", "건설", 13484800000000, ["엔지니어링"]),
    ("086450", "동국제약", "KOSDAQ", "바이오/제약", 1135300000000, ["제약"]),
    ("093320", "케이아이엔엑스", "KOSDAQ", "IT/소프트웨어", 855500000000, ["클라우드"]),
    ("089860", "롯데렌탈", "KOSPI", "서비스", 2124100000000, ["밸류업"]),
    ("159010", "아스플로", "KOSDAQ", "기계/장비", 498100000000, ["반도체장비"]),
    ("003230", "삼양식품", "KOSPI", "음식료", 9000000000000, ["K-푸드"]),
    ("004490", "세방전지", "KOSPI", "자동차부품", 1200000000000, ["밸류업"])
]


# ==============================================================================
# 2. 공통 유틸리티 및 KIS / DART 선택 연동 함수
# ==============================================================================
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
            print("  ✓ KIS Open API 토큰 발급 성공 (실수급 오실레이터 활성화)")
            return res.json().get("access_token")
    except Exception as e:
        print(f"  [안내] KIS 토큰 발급 실패({e}) -> 거래대금 기반 수급 오실레이터로 대체합니다.")
    return None


def fetch_dart_op_badge(dart_client, ticker: str):
    """DART_API_KEY가 설정된 경우 최근 분기 연결 영업이익 증감률 배지를 조회합니다."""
    if dart_client is None:
        return None
    try:
        cur_year = datetime.now().year
        for y in [cur_year, cur_year - 1]:
            for reprt_code in ["11014", "11012", "11013", "11011"]:
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
                                return ("영익 흑자전환", True)
                            if p_val < 0 and c_val < 0 and c_val > p_val:
                                return ("영익 적자축소", True)
                            if p_val > 0:
                                yoy = round(((c_val - p_val) / abs(p_val)) * 100, 1)
                                sign = "+" if yoy >= 0 else ""
                                return (f"영익 {sign}{yoy}%", yoy >= 0)
    except Exception:
        pass
    return None


# ==============================================================================
# 3. 한국·미국 종목 유니버스 빌더 및 개별 종목 스캔
# ==============================================================================
def build_kr_universe():
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
        print(f"  [안내] KRX 리스팅 대체({e}) -> 기본 대표 종목으로 진행합니다.")
        return [(c, n, ex, sec, format_marcap_kr(mc), tags, float(mc)) for c, n, ex, sec, mc, tags in FALLBACK_KR]


def build_us_universe():
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
    except Exception:
        pass
    return universe


def inspect_stock(ticker, name, exchange, industry, mcap, tags, marcap_num, country, start_date):
    """개별 종목의 시세를 분석해 종목 카드 데이터와 시장 전체 통계(산업강도·체온계·오실레이터용)를 함께 반환합니다."""
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

        # 20일 수익률 및 5일 전 대비 변화 (산업 강도판 계산용)
        close_20d_ago = float(close.iloc[-21]) if len(close) >= 21 else float(close.iloc[0])
        ret_20d = round(((latest_close - close_20d_ago) / close_20d_ago) * 100, 1) if close_20d_ago > 0 else 0.0
        close_5d_ago = float(close.iloc[-6]) if len(close) >= 6 else prev_close
        close_25d_ago = float(close.iloc[-26]) if len(close) >= 26 else float(close.iloc[0])
        ret_20d_prev5 = round(((close_5d_ago - close_25d_ago) / close_25d_ago) * 100, 1) if close_25d_ago > 0 else ret_20d

        # 일봉 20일·120일 이평선 & RSI(14)
        ma20_series = close.rolling(20).mean()
        ma120_series = close.rolling(120).mean()
        ma20_d = float(ma20_series.iloc[-1])
        ma120_d = float(ma120_series.iloc[-1])
        rsi14 = round(compute_rsi(close, 14), 1)

        # 거래량 기반 수급 오실레이터 추정치 (77거래일 기준 EMA12 - EMA26 -> Signal 9)
        recent_77 = df.tail(77).copy()
        signed_flow = recent_77["Close"].pct_change().fillna(0) * recent_77["Volume"]
        norm_flow = (signed_flow.rolling(5).sum() / (recent_77["Volume"].rolling(20).mean() * 5 + 1)) * 0.1
        ema12 = norm_flow.ewm(span=12, adjust=False).mean()
        ema26 = norm_flow.ewm(span=26, adjust=False).mean()
        macd_flow = ema12 - ema26
        sig9 = macd_flow.ewm(span=9, adjust=False).mean()
        osc_hist = (macd_flow - sig9).dropna()
        osc_val = round(float(osc_hist.iloc[-1]), 3) if not osc_hist.empty else 0.0
        osc_pct = int(round((osc_hist.rank(pct=True).iloc[-1]) * 100)) if len(osc_hist) >= 5 else 50

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

        daily_range = (df["High"] - df["Low"]) / df["Close"]
        recent_vol = float(daily_range.iloc[-20:].mean())
        prior_vol = float(daily_range.iloc[-80:-20].mean()) if len(daily_range) >= 80 else recent_vol
        vcp_ok = recent_vol <= prior_vol * 0.90

        vol_5d = float(df["Volume"].iloc[-5:].mean())
        vol_20d = float(df["Volume"].iloc[-20:].mean())
        vol_recover = vol_5d >= vol_20d * 1.05
        not_overheated = (rsi14 < 72.0) and (latest_close < ma20_d * 1.15)
        is_overheated = (rsi14 >= 75.0) or (latest_close >= ma20_d * 1.15)

        if align_weeks == 0:
            if latest_close > ma20_d and ma20_d > ma120_d and vol_recover:
                stage = "onset"
            else:
                stage = "exit"
        elif 1 <= align_weeks <= 6:
            stage = "onset"
        else:
            stage = "warning" if (drawdown_pct <= -15.0 or latest_close < float(w_ma4.iloc[-1])) else "progress"

        price_str = f"{int(latest_close):,}원" if country == "KR" else f"${latest_close:,.2f}"
        last_date = df.index[-1].strftime("%Y-%m-%d")

        # 시장 전체 통계용 요약 메타 (이탈 종목 포함)
        stat_meta = {
            "country": country,
            "ticker": ticker,
            "name": name,
            "industry": industry,
            "stage": stage,
            "ret_20d": ret_20d,
            "ret_20d_prev5": ret_20d_prev5,
            "above_ma20": latest_close > ma20_d,
            "rsi14": rsi14,
            "is_overheated": is_overheated,
            "osc_val": osc_val,
            "osc_pct": osc_pct,
            "close_str": price_str,
            "marcap_num": marcap_num,
            "last_date": last_date
        }

        if stage == "exit":
            return {"stat": stat_meta, "card": None}

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

        ma20_str = f"{int(ma20_d):,}원" if country == "KR" else f"${ma20_d:,.2f}"
        ma120_str = f"{int(ma120_d):,}원" if country == "KR" else f"${ma120_d:,.2f}"
        high_str = f"{int(high_52w):,}원" if country == "KR" else f"${high_52w:,.2f}"

        card = {
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
        return {"stat": stat_meta, "card": card}
    except Exception:
        return None


# ==============================================================================
# 4. 탭별 동적 계산 엔진 (산업 강도 · 수급오실레이터 · 시장 체온계 · 매크로 금리)
# ==============================================================================
def compute_industry_strength(kr_stats: list):
    """한국 스캔 종목들을 업종별로 집계해 강도 점수(0~100), 변화폭, 20일 평균 수익률을 자동 산출합니다."""
    if not kr_stats:
        return []
    df = pd.DataFrame(kr_stats)
    grouped = []
    for ind, grp in df.groupby("industry"):
        if len(grp) < 2:
            continue
        avg_ret = float(grp["ret_20d"].mean())
        prev_ret = float(grp["ret_20d_prev5"].mean())
        above_ratio = float(grp["above_ma20"].mean()) * 100.0
        raw_strength = avg_ret * 1.5 + above_ratio * 0.5
        delta = round((avg_ret - prev_ret) * 1.5, 1)
        above_cnt = int(grp["above_ma20"].sum())
        total_cnt = len(grp)
        grouped.append({
            "name": str(ind),
            "raw": raw_strength,
            "delta": delta,
            "ret20d": f"{'+' if avg_ret > 0 else ''}{avg_ret:.1f}%",
            "count": f"{above_cnt}/{total_cnt}종목"
        })

    if not grouped:
        return []

    grouped.sort(key=lambda x: x["raw"], reverse=True)
    n = len(grouped)
    for idx, item in enumerate(grouped):
        score = int(round(100 * (n - 1 - idx) / max(1, n - 1))) if n > 1 else 50
        item["score"] = score

    top4 = grouped[:4]
    for item in top4:
        item["group"] = "strong"
        item.pop("raw", None)

    bot2 = grouped[-2:] if len(grouped) >= 6 else grouped[4:]
    for item in bot2:
        item["group"] = "weak"
        item.pop("raw", None)

    return top4 + bot2


def compute_supply_oscillator(kr_stats: list, kis_token: str = None):
    """KIS 토큰이 있으면 실수급(외국인+기관 순매수)으로, 없으면 거래량 유입 오실레이터로 상위 8종목을 갱신합니다."""
    if not kr_stats:
        return []

    # 시총 상위 및 초입/진행 주요 종목 8개 선정
    active_kr = [s for s in kr_stats if s["stage"] in ("onset", "progress")]
    pool = active_kr if len(active_kr) >= 6 else kr_stats
    pool = sorted(pool, key=lambda x: x.get("marcap_num", 0), reverse=True)[:8]

    osc_list = []
    for item in pool:
        code = item["ticker"]
        name = item["name"]
        osc_num = item["osc_val"]
        pct = item["osc_pct"]

        # KIS API 토큰이 활성화된 경우 실제 외국인+기관 5일 누적 순매수대금 조회
        if kis_token and item.get("marcap_num", 0) > 0:
            try:
                url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor"
                headers = {
                    "authorization": f"Bearer {kis_token}",
                    "appkey": KIS_APP_KEY,
                    "appsecret": KIS_APP_SECRET,
                    "tr_id": "FHKST01010900"
                }
                res = requests.get(
                    url,
                    headers=headers,
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                    timeout=6
                )
                if res.ok:
                    rows = res.json().get("output", [])
                    if len(rows) >= 15:
                        net_vals = []
                        for r in reversed(rows[:30]):
                            f_amt = float(r.get("frgn_ntby_tr_pbmn", 0) or 0) * 1000.0
                            o_amt = float(r.get("orgn_ntby_tr_pbmn", 0) or 0) * 1000.0
                            net_vals.append(((f_amt + o_amt) / item["marcap_num"]) * 100.0)
                        s_net = pd.Series(net_vals).rolling(5).sum().dropna()
                        if len(s_net) >= 9:
                            e12 = s_net.ewm(span=12, adjust=False).mean()
                            e26 = s_net.ewm(span=26, adjust=False).mean()
                            macd = e12 - e26
                            sig = macd.ewm(span=9, adjust=False).mean()
                            hist = (macd - sig).dropna()
                            osc_num = round(float(hist.iloc[-1]), 3)
                            pct = int(round(hist.rank(pct=True).iloc[-1] * 100))
                time.sleep(0.08)
            except Exception:
                pass

        sign = "+" if osc_num >= 0 else ""
        status = "유입강화" if pct >= 65 else ("약화" if pct <= 35 else "중립")
        osc_list.append({
            "code": code,
            "name": name,
            "oscValue": f"{sign}{osc_num:.3f}%",
            "oscNumeric": osc_num,
            "axisLabel": f"{sign}{osc_num:.2f}%",
            "status": status,
            "percentile": pct,
            "close": item["close_str"]
        })
    return osc_list


def compute_macro_yields(start_date: str):
    """FRED 미국 국채 2년(DGS2)·10년(DGS10)·30년(DGS30) 주봉 이동평균과 정배열 여부를 자동 계산합니다."""
    series_map = [
        ("FRED:DGS2", "미 국채 2년 (DGS2)"),
        ("FRED:DGS10", "미 국채 10년 (DGS10)"),
        ("FRED:DGS30", "미 국채 30년 (DGS30)")
    ]
    macro_start = (datetime.now() - timedelta(days=420)).strftime("%Y-%m-%d")
    results = []
    for sym, title in series_map:
        try:
            df = fdr.DataReader(sym, macro_start)
            if df is None or df.empty:
                continue
            col = df.columns[0]
            s = df[col].dropna()
            if len(s) < 30:
                continue
            cur_val = round(float(s.iloc[-1]), 2)
            prev_1m = float(s.iloc[-22]) if len(s) >= 22 else float(s.iloc[0])
            diff_bp = int(round((cur_val - prev_1m) * 100))
            bp_sign = "+" if diff_bp >= 0 else ""

            w_s = s.resample("W-FRI").last().dropna()
            ma4 = round(float(w_s.rolling(4).mean().iloc[-1]), 2) if len(w_s) >= 4 else cur_val
            ma13 = round(float(w_s.rolling(13).mean().iloc[-1]), 2) if len(w_s) >= 13 else ma4
            ma26 = round(float(w_s.rolling(26).mean().iloc[-1]), 2) if len(w_s) >= 26 else ma13
            ma52 = round(float(w_s.rolling(52).mean().iloc[-1]), 2) if len(w_s) >= 52 else ma26
            aligned = (ma4 >= ma13) and (ma13 >= ma26) and (ma26 >= ma52)

            results.append({
                "title": title,
                "current": f"{cur_val:.2f}%",
                "change1m": f"1개월 {bp_sign}{diff_bp}bp",
                "badge": "주봉 정배열" if aligned else "주봉 비정배열",
                "aligned": aligned,
                "ma4": ma4,
                "ma13": ma13,
                "ma26": ma26,
                "ma52": ma52,
                "val": cur_val
            })
        except Exception as e:
            print(f"  [경고] {title} 수집 실패: {e}")
    return results


def compute_thermometer(kr_stats: list, macro_yields: list):
    """코스피 지수 및 스캔 종목 통계로 시장 체온계 5대 축과 가중 종합 점수를 매일 자동 산출합니다."""
    if not kr_stats:
        return [], 50.0

    total_n = max(1, len(kr_stats))
    onset_n = sum(1 for s in kr_stats if s["stage"] == "onset")
    prog_n = sum(1 for s in kr_stats if s["stage"] == "progress")
    exit_n = sum(1 for s in kr_stats if s["stage"] == "exit")
    overheat_n = sum(1 for s in kr_stats if s["is_overheated"])
    avg_rsi = round(sum(s["rsi14"] for s in kr_stats) / total_n, 1)

    # 1. 밸류에이션 (25%): 코스피 52주 밴드 내 위치
    val_score = 55.0
    try:
        ks = fdr.DataReader("KS11", (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
        if ks is not None and not ks.empty:
            c = ks["Close"].dropna()
            cur_k, hi_k, lo_k = float(c.iloc[-1]), float(c.max()), float(c.min())
            if hi_k > lo_k:
                val_score = round(((cur_k - lo_k) / (hi_k - lo_k)) * 100, 1)
    except Exception:
        pass

    # 2. 신용·유동성 (30%): 미 국채 10년물 금리 레벨 기반 (3.5%~5.5% 구간 정규화)
    dgs10 = next((m["val"] for m in macro_yields if "10년" in m["title"]), 4.5)
    liq_score = round(min(100.0, max(10.0, (5.5 - float(dgs10)) / 2.0 * 100.0)), 1)

    # 3. 심리·포지셔닝 (20%): 전체 스캔 종목 평균 RSI(14)
    sent_score = round(min(100.0, max(0.0, avg_rsi)), 1)

    # 4. 모멘텀·브레드스 (15%): 정배열(초입+진행) 종목 비율
    breadth_score = round(((onset_n + prog_n) / total_n) * 100.0, 1)

    # 5. 투기 열기 (10%): 단기 과열(RSI>=75 또는 20일선 이격 +15% 이상) 비율 × 2.5 스케일
    spec_score = round(min(100.0, (overheat_n / total_n) * 250.0), 1)

    def label_for(sc):
        if sc >= 70:
            return "과열 경계"
        if sc <= 35:
            return "차가움 (우호적)"
        return "중립"

    axes = [
        {"name": "밸류에이션", "weight": "25%", "score": val_score, "status": label_for(val_score), "desc": "코스피 52주 밴드 내 지수 레벨 위치"},
        {"name": "신용·유동성", "weight": "30%", "score": liq_score, "status": label_for(liq_score), "desc": f"미 국채 10년물({dgs10:.2f}%) 연동 유동성 여건"},
        {"name": "심리·포지셔닝", "weight": "20%", "score": sent_score, "status": label_for(sent_score), "desc": f"스캔 종목 평균 RSI({avg_rsi}) 기반 투자 심리"},
        {"name": "모멘텀·브레드스", "weight": "15%", "score": breadth_score, "status": label_for(breadth_score), "desc": f"정배열 비율 (초입 {onset_n} · 진행 {prog_n} vs 이탈 {exit_n})"},
        {"name": "투기 열기", "weight": "10%", "score": spec_score, "status": label_for(spec_score), "desc": f"단기 과열(RSI≥75·이격과다) 종목 비중 ({overheat_n}/{total_n}종목)"}
    ]
    comp = round(val_score * 0.25 + liq_score * 0.30 + sent_score * 0.20 + breadth_score * 0.15 + spec_score * 0.10, 1)
    return axes, comp


def scan_universe(universe, country, start_date):
    stats, cards = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(inspect_stock, t, n, ex, sec, mcap, tags, mnum, country, start_date)
            for (t, n, ex, sec, mcap, tags, mnum) in universe
        ]
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                stats.append(res["stat"])
                if res["card"] is not None:
                    cards.append(res["card"])
    return stats, cards


def main():
    print("=" * 65)
    print(" 주도주 스크리너 - 5대 탭 전체(종목·수급·산업·체온·매크로) 자동 갱신")
    print("=" * 65)

    start_date = (datetime.now() - timedelta(days=270)).strftime("%Y-%m-%d")

    print("\n[1/4] 한국(KRX) 시총·거래대금 상위 유니버스 스캔 중...")
    kr_univ = build_kr_universe()
    kr_stats, kr_cards = scan_universe(kr_univ, "KR", start_date)

    print("\n[2/4] 미국(S&P500 + 핵심주) 유니버스 스캔 중...")
    us_univ = build_us_universe()
    us_stats, us_cards = scan_universe(us_univ, "US", start_date)

    all_cards = kr_cards + us_cards
    if not all_cards:
        print("\n[경고] 수집된 종목이 없어 기존 data.json을 유지합니다.")
        return

    # 선택적 DART API 연동 (상위 초입/진행 한국 카드 대상 영업이익 배지 보강)
    dart_client = None
    if DART_API_KEY and OpenDartReader is not None:
        try:
            dart_client = OpenDartReader(DART_API_KEY)
            print("  ✓ OPEN DART API 활성화 완료 (분기 영업이익 배지 연동)")
        except Exception:
            dart_client = None

    stage_counts = {
        "KR": {"onset": 0, "progress": 0, "warning": 0},
        "US": {"onset": 0, "progress": 0, "warning": 0}
    }
    final_stocks = []
    for c in ["KR", "US"]:
        for st in ["onset", "progress", "warning"]:
            subset = [s for s in all_cards if s["country"] == c and s["stage"] == st]
            stage_counts[c][st] = len(subset)
            subset.sort(key=lambda x: x.get("_sortKey", 0), reverse=True)
            top_subset = subset[:MAX_CARDS_PER_STAGE]
            for idx, s in enumerate(top_subset):
                s.pop("_sortKey", None)
                s["sectorRank"] = f"#{idx + 1}"
                if c == "KR" and dart_client is not None and idx < 10:
                    op_info = fetch_dart_op_badge(dart_client, s["ticker"])
                    if op_info:
                        s["opBadge"], s["opPositive"] = op_info
                    time.sleep(0.08)
                final_stocks.append(s)

    print("\n[3/4] 산업 강도판 및 수급 오실레이터 자동 계산 중...")
    kis_token = get_kis_access_token()
    industry_data = compute_industry_strength(kr_stats)
    oscillator_data = compute_supply_oscillator(kr_stats, kis_token=kis_token)

    print("\n[4/4] FRED 미 국채 금리(매크로) 및 시장 체온계 자동 계산 중...")
    macro_yields = compute_macro_yields(start_date)
    thermometer_axes, thermometer_total = compute_thermometer(kr_stats, macro_yields)

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
            "viewCount": existing.get("meta", {}).get("viewCount", 2183) + 1,
            "thermometerScore": thermometer_total
        },
        "TAB_META": {
            "KR": {
                "onset": {"label": "초입", "realTotal": stage_counts["KR"]["onset"], "desc": f"코스피·코스닥 상위 {len(kr_univ)}종목 중 정배열 초입 포착"},
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
        "OSCILLATOR_DATA": oscillator_data if oscillator_data else existing.get("OSCILLATOR_DATA", []),
        "INDUSTRY_DATA": industry_data if industry_data else existing.get("INDUSTRY_DATA", []),
        "THERMOMETER_AXES": thermometer_axes if thermometer_axes else existing.get("THERMOMETER_AXES", []),
        "MACRO_YIELDS": macro_yields if macro_yields else existing.get("MACRO_YIELDS", []),
        "SIMULATOR_RULES": existing.get("SIMULATOR_RULES", [])
    }

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(updated_payload, f, ensure_ascii=False, indent=2)

    print(f"\n[전체 갱신 완료] 기준일: {latest_price_date} | 시장 체온: {thermometer_total}도 | 산업강도 {len(industry_data)}개 | 매크로 {len(macro_yields)}개")


if __name__ == "__main__":
    main()
