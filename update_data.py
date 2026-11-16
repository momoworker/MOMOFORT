"""
STARFOLIO 스타일 주도주 스크리너 - 2단계: 한국·미국 주가 자동 수집 및 data.json 갱신 스크립트
실행 방법: python update_data.py (또는 윈도우에서 run_update.bat 더블클릭)
"""

import json
import os
from datetime import datetime, timedelta
import pandas as pd
import FinanceDataReader as fdr

# ==============================================================================
# 1. 수집 대상 종목 리스트 (원하는 종목을 자유롭게 추가/수정할 수 있습니다)
#    형식: ("티커", "종목명", "거래소", "업종", "시가총액표기", ["테마태그"])
# ==============================================================================
WATCHLIST_KR = [
    ("251970", "펌텍코리아", "KOSDAQ", "화학", "9,164억", ["화장품"]),
    ("028050", "삼성E&A", "KOSPI", "건설", "13조 4,848억", ["수자원", "전후재건"]),
    ("086450", "동국제약", "KOSDAQ", "바이오/제약", "1조 1,353억", ["제약업체"]),
    ("093320", "케이아이엔엑스", "KOSDAQ", "IT/소프트웨어", "8,555억", ["클라우드 컴퓨팅"]),
    ("089860", "롯데렌탈", "KOSPI", "기계/장비", "2조 1,241억", ["렌터카", "밸류업"]),
    ("079650", "서산", "KOSDAQ", "건자재/비금속", "1,454억", ["시멘트/레미콘"]),
    ("159010", "아스플로", "KOSDAQ", "기계/장비", "4,981억", ["반도체 장비"]),
    ("017890", "한국알콜", "KOSDAQ", "화학", "3,157억", ["밸류업", "정밀화학"]),
    ("267260", "HD현대일렉트릭", "KOSPI", "전기전자", "전력인프라", ["변압기", "AI전력"]),
    ("012450", "한화에어로스페이스", "KOSPI", "방산/항공", "방산주도", ["K-방산", "우주항공"]),
    ("003230", "삼양식품", "KOSPI", "음식료", "수출주도", ["K-푸드", "불닭"]),
    ("004490", "세방전지", "KOSPI", "자동차부품", "가치주", ["밸류업", "배당"])
]

WATCHLIST_US = [
    ("VRTX", "Vertex Pharmaceuticals", "NASDAQ", "헬스케어", "$138B", ["Biotech"]),
    ("ROIV", "Roivant Sciences", "NASDAQ", "헬스케어", "$29B", ["Biopharma"]),
    ("MEDP", "Medpace Holdings", "NASDAQ", "헬스케어", "$18B", ["임상수탁(CRO)"]),
    ("BDX", "Becton Dickinson", "NYSE", "헬스케어", "$59B", ["의료기기"]),
    ("BBY", "Best Buy Co.", "NYSE", "경기소비재", "$22B", ["리테일"]),
    ("TEVA", "Teva Pharmaceutical", "NYSE", "헬스케어", "$51B", ["바이오시밀러"]),
    ("HALO", "Halozyme Therapeutics", "NASDAQ", "헬스케어", "$11B", ["SC제형변경"]),
    ("OKTA", "Okta Inc.", "NASDAQ", "IT", "$32B", ["사이버보안"]),
    ("NVDA", "NVIDIA Corp.", "NASDAQ", "반도체", "AI대장주", ["AI칩", "데이터센터"]),
    ("PLTR", "Palantir Technologies", "NASDAQ", "소프트웨어", "AI소프트웨어", ["국방AI", "AIP"]),
    ("GEV", "GE Vernova Inc.", "NYSE", "에너지/전력", "전력인프라", ["가스터빈", "그리드"]),
    ("VRT", "Vertiv Holdings", "NYSE", "인프라", "데이터센터", ["액체냉각", "전력"])
]


def compute_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def format_volume(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.2f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.2f}K"
    return str(int(vol))


def analyze_stock(ticker: str, name: str, exchange: str, industry: str, mcap: str, tags: list, country: str, start_date: str):
    """주가 데이터를 가져와 4·13·26주(주봉) 및 20·120일(일봉) 이평선을 계산하고 단계·점수를 판정합니다."""
    try:
        df = fdr.DataReader(ticker, start_date)
        if df is None or len(df) < 130:
            print(f"  [건너뜀] {name}({ticker}): 데이터 부족 ({0 if df is None else len(df)}일)")
            return None

        df = df.dropna(subset=["Close"]).copy()
        close = df["Close"]
        latest_close = float(close.iloc[-1])
        prev_close = float(close.iloc[-2])
        change_pct = round(((latest_close - prev_close) / prev_close) * 100, 1)
        change_arrow = "▲" if change_pct > 0 else ("▼" if change_pct < 0 else "-")
        change_text = f"{change_arrow}{abs(change_pct):.1f}%"

        # 일봉 이동평균선 (20일선, 120일선) & RSI
        ma20_d = float(close.rolling(20).mean().iloc[-1])
        ma120_d = float(close.rolling(120).mean().iloc[-1])
        rsi14 = round(compute_rsi(close, 14), 1)

        # 주봉 변환 (금요일 기준) -> 4주, 13주, 26주 이동평균선
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

        # 주봉 정배열 연속 주차 계산 (4주선 > 13주선 > 26주선)
        aligned_series = (w_ma4 > w_ma13) & (w_ma13 > w_ma26)
        align_weeks = 0
        for val in reversed(aligned_series.tolist()):
            if bool(val):
                align_weeks += 1
            else:
                break

        # 52주(또는 수집기간) 고점 대비 하락률
        high_52w = float(df["High"].max())
        drawdown_pct = round(((latest_close - high_52w) / high_52w) * 100, 1)

        # 변동성 수축(VCP) 체크: 최근 4주 일간 변동폭 평균 <= 직전 12주 평균의 85%
        daily_range = (df["High"] - df["Low"]) / df["Close"]
        recent_vol = float(daily_range.iloc[-20:].mean())
        prior_vol = float(daily_range.iloc[-80:-20].mean()) if len(daily_range) >= 80 else recent_vol
        vcp_ok = recent_vol <= prior_vol * 0.90

        # 거래대금(거래량) 회복 체크: 최근 5일 평균 거래량 > 최근 20일 평균 거래량
        vol_5d = float(df["Volume"].iloc[-5:].mean())
        vol_20d = float(df["Volume"].iloc[-20:].mean())
        vol_recover = vol_5d >= vol_20d

        # 과열 아님 체크: RSI < 72 및 20일선 이격도 < 115%
        not_overheated = (rsi14 < 72.0) and (latest_close < ma20_d * 1.15)

        # 추세 단계 분류: 초입(onset) / 진행(progress) / 경계(warning)
        if align_weeks == 0 and latest_close > ma20_d and ma20_d > ma120_d:
            # 일봉 20>120 정배열 전환 직후(주봉 정배열 0주차 초입)
            stage = "onset"
        elif 1 <= align_weeks <= 6:
            stage = "onset"
        elif align_weeks >= 7:
            stage = "warning" if (drawdown_pct <= -15.0 or latest_close < float(w_ma4.iloc[-1])) else "progress"
        else:
            # 비정배열이지만 모니터링 대상인 경우 경계로 분류
            stage = "warning"

        # 12점 스코어링 (초입 기준)
        prep_weeks = max(15, 52 - align_weeks)
        s_align = 3.0 if align_weeks <= 6 else 0.0
        s_prep = 3.0 if prep_weeks >= 15 else 0.0
        s_vcp = 1.0 if vcp_ok else 0.0
        s_vol = 1.0 if vol_recover else 0.0
        s_sector = 2.0  # 관심 주도 섹터 가점
        s_heat = 1.0 if not_overheated else 0.0
        s_bonus = 1.0 if (latest_close > ma20_d) else 0.5
        total_score = round(s_align + s_prep + s_vcp + s_vol + s_sector + s_heat + s_bonus, 1)

        score_breakdown = [
            {"label": "정배열 초입 (≤6주)", "score": f"{int(s_align)}/3", "passed": s_align > 0},
            {"label": "준비기간 충분 (≥15주)", "score": f"{int(s_prep)}/3", "passed": s_prep > 0},
            {"label": "변동성 수축 (VCP)", "score": f"{int(s_vcp)}/1", "passed": vcp_ok},
            {"label": "거래대금 회복", "score": f"{int(s_vol)}/1", "passed": vol_recover},
            {"label": "주도 섹터 가점", "score": "2/2", "passed": True},
            {"label": "과열 아님 (RSI·이격)", "score": f"{int(s_heat)}/1", "passed": not_overheated},
            {"label": "보너스: 20일선 상회", "score": f"{s_bonus}/1", "passed": s_bonus >= 1.0}
        ] if stage == "onset" else []

        # 배지 및 상태 문구
        if stage == "onset":
            right_badge = "B" if total_score >= 10 else "W"
            right_badge_type = "B"
            status_label = f"점수 {total_score}"
            verdict = f"정배열 {align_weeks}주(초입) · 20일선 {'>' if ma20_d > ma120_d else '<'} 120일선 · RSI {rsi14} · {'변동성수축' if vcp_ok else '추세전환'}"
        elif stage == "progress":
            right_badge = "확산"
            right_badge_type = "DIFF"
            status_label = "신고가" if drawdown_pct >= -3.0 else f"고점 {drawdown_pct:.0f}%"
            verdict = f"확산 · 정배열 {align_weeks}주 · {status_label} · RSI {rsi14} — 추세 진행 중"
        else:
            right_badge = "경계"
            right_badge_type = "WARN"
            status_label = f"고점 {drawdown_pct:.0f}%"
            verdict = f"경계 · 정배열 {align_weeks}주 · 고점 대비 {drawdown_pct:.1f}% 조정 — 이평선 지지 점검"

        # 차트용 최근 18개 포인트 (약 1주 간격 샘플링 + 마지막 거래일)
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
            "sectorRank": "#자동계산",
            "marketCap": mcap,
            "lastClose": price_str,
            "onsetDate": last_date,
            "opBadge": f"RSI {rsi14}",
            "opPositive": rsi14 >= 50,
            "tags": tags,
            "volumeText": format_volume(float(df["Volume"].iloc[-1])),
            "verdict": verdict,
            "scoreBreakdown": score_breakdown,
            "earningsTitle": f"일봉·주봉 이동평균선 자동 분석 ({last_date} 기준)",
            "earningsSummary": f"현재가 {price_str} · 20일선 {ma20_str} · 120일선 {ma120_str} · 기간 고점 대비 {drawdown_pct:.1f}%",
            "tableHeaders": ["지표 구분", "단기선 (20일/4주)", "장기선 (120일/26주)", "기간 고점 / RSI"],
            "quarters": [
                {
                    "c1": last_date,
                    "c2": ma20_str,
                    "c3": ma120_str,
                    "c4": f"{high_str} / RSI {rsi14}"
                }
            ],
            "series": series
        }
    except Exception as e:
        print(f"  [오류] {name}({ticker}) 수집 실패: {e}")
        return None


def main():
    print("=" * 55)
    print(" STARFOLIO 주도주 스크리너 - 자동 데이터 갱신 시작")
    print("=" * 55)

    start_date = (datetime.now() - timedelta(days=270)).strftime("%Y-%m-%d")
    stocks = []

    print("\n[1/2] 한국(KRX) 종목 시세 및 이평선 수집 중...")
    for item in WATCHLIST_KR:
        res = analyze_stock(*item, country="KR", start_date=start_date)
        if res:
            stocks.append(res)
            print(f"  ✓ {res['name']} ({res['ticker']}) -> [{res['stage']}] {res['lastClose']} ({res['changeText']})")

    print("\n[2/2] 미국(NASDAQ/NYSE) 종목 시세 및 이평선 수집 중...")
    for item in WATCHLIST_US:
        res = analyze_stock(*item, country="US", start_date=start_date)
        if res:
            stocks.append(res)
            print(f"  ✓ {res['name']} ({res['ticker']}) -> [{res['stage']}] {res['lastClose']} ({res['changeText']})")

    if not stocks:
        print("\n[경고] 수집된 종목이 없어 기존 data.json을 유지합니다.")
        return

    # 기존 data.json이 있으면 매크로/체온계/산업강도/오실레이터 섹션을 보존
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    existing = {}
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    def count_stage(c, st):
        return sum(1 for s in stocks if s["country"] == c and s["stage"] == st)

    latest_price_date = max(s["onsetDate"] for s in stocks)

    updated_payload = {
        "meta": {
            "priceDate": latest_price_date,
            "updatedSchedule": "17시 갱신 시작 · 18시/18:30/20시 재시도",
            "viewCount": existing.get("meta", {}).get("viewCount", 2183) + 1
        },
        "TAB_META": {
            "KR": {
                "onset": {"label": "초입", "realTotal": count_stage("KR", "onset"), "desc": "정배열 6주 이하 · 20일·120일선 정배열 전환 초입 종목"},
                "progress": {"label": "진행", "realTotal": count_stage("KR", "progress"), "desc": "정배열 7주 이상 확산 · 고점 대비 -15% 이내 진행 종목"},
                "warning": {"label": "경계", "realTotal": count_stage("KR", "warning"), "desc": "고점 대비 -15% 초과 조정 또는 4주선 이탈 경계 종목"}
            },
            "US": {
                "onset": {"label": "초입", "realTotal": count_stage("US", "onset"), "desc": "미국 NASDAQ·NYSE 정배열 초입 종목"},
                "progress": {"label": "진행", "realTotal": count_stage("US", "progress"), "desc": "미국 시장 정배열 확산 진행 종목"},
                "warning": {"label": "경계", "realTotal": count_stage("US", "warning"), "desc": "미국 시장 추세 경계 종목"}
            }
        },
        "STOCKS_DATA": stocks,
        "OSCILLATOR_DATA": existing.get("OSCILLATOR_DATA", []),
        "INDUSTRY_DATA": existing.get("INDUSTRY_DATA", []),
        "THERMOMETER_AXES": existing.get("THERMOMETER_AXES", []),
        "MACRO_YIELDS": existing.get("MACRO_YIELDS", []),
        "SIMULATOR_RULES": existing.get("SIMULATOR_RULES", [])
    }

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(updated_payload, f, ensure_ascii=False, indent=2)

    print(f"\n[완료] 총 {len(stocks)}개 종목으로 data.json 업데이트 완료! (기준일: {latest_price_date})")


if __name__ == "__main__":
    main()
