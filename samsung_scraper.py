"""삼성전자 보통주/우선주 시세 + 비교 추적 (일일 리포트).

- 보통주 005930.KS, 우선주 005935.KS yfinance 일별 종가 조회
- 괴리율 (보통주 대비 우선주 할인) = (보통주 - 우선주) / 보통주 × 100%
- 30일 추이 차트 2종 (주가, 괴리율) + 30일 통계 (평균/최저/최고/변동폭)
- 이메일 (Gmail SMTP) + 텔레그램 + GH Pages 사본 발송
- '삼성전자' 관련 Google News + Gemini AI 요약
"""

from __future__ import annotations

import csv
import html
import io
import os
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

# Windows 콘솔 한글/이모지 출력을 위한 UTF-8 재설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import matplotlib
matplotlib.use("Agg")  # 헤드리스 렌더링 (GH Actions 환경)
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

import requests  # noqa: E402
import yfinance as yf  # noqa: E402

from actions_utils import already_sent_today_kst  # noqa: E402
from mailer import send_html_email  # noqa: E402
from news_utils import strip_source_suffix  # noqa: E402
from notifier import send_telegram_message  # noqa: E402
from summarizer import render_summary_block_html, summarize_titles  # noqa: E402


KST = timezone(timedelta(hours=9))
OUTPUT_DIR = Path(__file__).parent / "output"
DB_PATH = OUTPUT_DIR / "SAMSUNG_history.db"
PUBLIC_DIR = OUTPUT_DIR / "public" / "samsung"
PUBLIC_REPORT_PATH = PUBLIC_DIR / "index.html"
PUBLIC_REPORT_URL = "https://xerostyle00.github.io/daily-digest/samsung/"

PRICE_CHART_CID = "chart_price"
DISCOUNT_CHART_CID = "chart_discount"

NEWS_QUERY = "삼성전자"
NEWS_LIMIT = 10
CHART_DAYS = 30
GOOGLE_NEWS_RSS = (
    "https://news.google.com/rss/search"
    "?q={query}&hl=ko&gl=KR&ceid=KR:ko"
)

TICKERS = {
    "보통주": "005930.KS",
    "우선주": "005935.KS",
}
TICKER_COLORS = {
    "보통주": "#42a5f5",  # blue
    "우선주": "#ffb74d",  # orange
}
DISCOUNT_COLOR = "#9c27b0"  # purple


# ────────────────────────────────────────────────────────────
# 데이터 수집
# ────────────────────────────────────────────────────────────

def fetch_prices() -> list[dict]:
    """yfinance 로 각 ticker 일별 종가 조회. CHART_DAYS + 여유분 반환."""
    rows: list[dict] = []
    for label, ticker in TICKERS.items():
        hist = yf.Ticker(ticker).history(period=f"{CHART_DAYS + 5}d", interval="1d")
        if hist.empty:
            rows.append({"label": label, "ticker": ticker, "error": "no data"})
            continue
        for idx, (ts, r) in enumerate(hist.iterrows()):
            prev_close = float(hist.iloc[idx - 1]["Close"]) if idx > 0 else float(r["Close"])
            close = float(r["Close"])
            change = close - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0.0
            rows.append({
                "label": label,
                "ticker": ticker,
                "date": ts.strftime("%Y-%m-%d"),
                "close": round(close, 2),
                "prev_close": round(prev_close, 2),
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
            })
    return rows


def fetch_news(query: str, limit: int) -> list[dict]:
    url = GOOGLE_NEWS_RSS.format(query=quote(query))
    resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    items = root.findall(".//item")[:limit]
    news: list[dict] = []
    for item in items:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        source_el = item.find("source")
        source = source_el.text.strip() if source_el is not None and source_el.text else ""
        news.append({
            "title": strip_source_suffix(title, source),
            "source": source,
            "pub_date": pub_date,
            "link": link,
            "query": query,
        })
    return news


# ────────────────────────────────────────────────────────────
# SQLite 저장소
# ────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices (
            ticker      TEXT NOT NULL,
            label       TEXT NOT NULL,
            date        TEXT NOT NULL,
            close       REAL NOT NULL,
            prev_close  REAL NOT NULL,
            change      REAL NOT NULL,
            change_pct  REAL NOT NULL,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS news (
            link        TEXT PRIMARY KEY,
            title       TEXT NOT NULL,
            source      TEXT,
            pub_date    TEXT,
            query       TEXT,
            captured_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
        CREATE INDEX IF NOT EXISTS idx_news_captured ON news(captured_at);
        """
    )
    conn.commit()


def upsert_prices(conn: sqlite3.Connection, rows: list[dict], captured_at: str) -> None:
    payload = [
        (
            r["ticker"], r["label"], r["date"], r["close"], r["prev_close"],
            r["change"], r["change_pct"], captured_at,
        )
        for r in rows if "error" not in r
    ]
    if not payload:
        return
    conn.executemany(
        """
        INSERT INTO prices (ticker, label, date, close, prev_close, change, change_pct, captured_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, date) DO UPDATE SET
            close=excluded.close,
            prev_close=excluded.prev_close,
            change=excluded.change,
            change_pct=excluded.change_pct,
            captured_at=excluded.captured_at
        """,
        payload,
    )
    conn.commit()


def upsert_news(conn: sqlite3.Connection, rows: list[dict], captured_at: str) -> None:
    payload = [
        (n["link"], n["title"], n["source"], n["pub_date"], n["query"], captured_at)
        for n in rows if n.get("link")
    ]
    if not payload:
        return
    conn.executemany(
        """
        INSERT INTO news (link, title, source, pub_date, query, captured_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(link) DO NOTHING
        """,
        payload,
    )
    conn.commit()


def load_latest_prices(conn: sqlite3.Connection) -> list[dict]:
    rows: list[dict] = []
    for label, ticker in TICKERS.items():
        cursor = conn.execute(
            """
            SELECT label, ticker, date, close, prev_close, change, change_pct
            FROM prices WHERE ticker = ? ORDER BY date DESC LIMIT 1
            """,
            (ticker,),
        )
        row = cursor.fetchone()
        if row:
            rows.append({
                "label": row[0], "ticker": row[1], "date": row[2],
                "close": row[3], "prev_close": row[4],
                "change": row[5], "change_pct": row[6],
            })
    return rows


def load_history_for_chart(conn: sqlite3.Connection, days: int) -> dict:
    """{label: [{date, close}, ...]} ascending date order."""
    result: dict[str, list[dict]] = {}
    for label, ticker in TICKERS.items():
        cursor = conn.execute(
            """
            SELECT date, close FROM prices
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (ticker, days),
        )
        series = [{"date": d, "close": c} for d, c in cursor.fetchall()]
        series.reverse()
        result[label] = series
    return result


def load_recent_news(conn: sqlite3.Connection, limit: int) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT title, source, pub_date, link, captured_at
        FROM news ORDER BY captured_at DESC, pub_date DESC LIMIT ?
        """,
        (limit,),
    )
    return [
        {"title": r[0], "source": r[1], "pub_date": r[2], "link": r[3], "captured_at": r[4]}
        for r in cursor.fetchall()
    ]


# ────────────────────────────────────────────────────────────
# 통계 & 괴리율
# ────────────────────────────────────────────────────────────

def _stats_from_chart(chart: dict) -> dict[str, dict]:
    """{label: {min, max, avg, range_pct}} per ticker."""
    out: dict[str, dict] = {}
    for label, series in chart.items():
        if not series:
            continue
        closes = [p["close"] for p in series]
        vmin = min(closes)
        vmax = max(closes)
        avg = sum(closes) / len(closes)
        range_pct = ((vmax - vmin) / vmin * 100) if vmin else 0.0
        out[label] = {"min": vmin, "max": vmax, "avg": avg, "range_pct": range_pct}
    return out


def _compute_discount_series(chart: dict) -> list[tuple[str, float]]:
    """[(date, discount_pct), ...] — 보통주·우선주 모두 데이터 있는 날짜만."""
    common = {p["date"]: p["close"] for p in chart.get("보통주", [])}
    preferred = {p["date"]: p["close"] for p in chart.get("우선주", [])}
    series: list[tuple[str, float]] = []
    for date in sorted(common.keys() & preferred.keys()):
        c = common[date]
        p = preferred[date]
        if c:
            series.append((date, (c - p) / c * 100))
    return series


def _discount_stats(chart: dict) -> dict | None:
    """{current, avg, min, max} (%). 데이터 부족 시 None."""
    series = _compute_discount_series(chart)
    if not series:
        return None
    values = [v for _, v in series]
    return {
        "current": series[-1][1],
        "avg": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
    }


# ────────────────────────────────────────────────────────────
# 차트 PNG 렌더
# ────────────────────────────────────────────────────────────

def render_price_chart_png(chart: dict) -> bytes:
    """30일 주가 추이 PNG (보통주 + 우선주, Y축 자동 스케일).

    삼성전자는 액면분할/주가 변동 폭이 크므로 고정 Y축이 부적절. matplotlib
    auto-scale 에 맡기되, 천 단위 콤마로 포매팅.
    """
    series_data = {label: s for label, s in chart.items() if len(s) >= 2}
    if not series_data:
        return b""

    max_dates_series = max(series_data.values(), key=len)
    max_dates = [p["date"] for p in max_dates_series]

    fig, ax = plt.subplots(figsize=(8.2, 2.6), dpi=140)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for label, series in series_data.items():
        x = list(range(len(series)))
        y = [p["close"] for p in series]
        ax.plot(x, y, color=TICKER_COLORS.get(label, "#666"),
                linewidth=2.0, solid_capstyle="round")

    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    n_points = len(max_dates)
    if n_points > 1:
        step = max(1, (n_points - 1) // 6)
        ticks = list(range(0, n_points, step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([max_dates[i][5:] for i in ticks], fontsize=8)

    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, alpha=0.3, linewidth=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#ccc")
    fig.tight_layout(pad=0.6)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor="white")
    plt.close(fig)
    return buf.getvalue()


def render_discount_chart_png(chart: dict) -> bytes:
    """30일 괴리율 추이 PNG (별도 mini chart, Y축 %)."""
    series = _compute_discount_series(chart)
    if len(series) < 2:
        return b""

    dates = [d for d, _ in series]
    values = [v for _, v in series]

    fig, ax = plt.subplots(figsize=(8.2, 2.0), dpi=140)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    x = list(range(len(values)))
    ax.plot(x, values, color=DISCOUNT_COLOR, linewidth=2.0, solid_capstyle="round")
    ax.fill_between(x, values, alpha=0.1, color=DISCOUNT_COLOR)

    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.1f}%"))

    n_points = len(dates)
    if n_points > 1:
        step = max(1, (n_points - 1) // 6)
        ticks = list(range(0, n_points, step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([dates[i][5:] for i in ticks], fontsize=8)

    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, alpha=0.3, linewidth=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#ccc")
    fig.tight_layout(pad=0.6)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor="white")
    plt.close(fig)
    return buf.getvalue()


# ────────────────────────────────────────────────────────────
# 이메일 HTML 렌더
# ────────────────────────────────────────────────────────────

def render_chart_legend(labels: list[str]) -> str:
    items = "".join(
        f'<span style="display:inline-block;margin-right:16px">'
        f'<span style="display:inline-block;width:10px;height:10px;'
        f'background:{TICKER_COLORS.get(label, "#666")};'
        f'vertical-align:middle;margin-right:6px"></span>'
        f'<span style="vertical-align:middle;font-size:13px;color:#444">'
        f'{html.escape(label)}</span>'
        f"</span>"
        for label in labels
    )
    return (
        '<div style="margin:12px 0 4px 0">'
        f"{items}"
        '<span style="color:#999;font-size:11px;margin-left:8px">'
        "(일별 종가, 단위: 원)</span>"
        "</div>"
    )


def render_email_html(latest: list[dict], news: list[dict], chart: dict,
                      summary: list[str] | None = None,
                      *, has_price_image: bool = False,
                      has_discount_image: bool = False) -> str:
    today = datetime.now(KST).strftime("%Y.%m.%d")
    stats = _stats_from_chart(chart)
    discount = _discount_stats(chart)

    ref_dates = sorted({r["date"] for r in latest if r.get("date")}, reverse=True)
    ref_caption = (
        f" <span style='color:#888;font-size:13px;font-weight:normal'>"
        f"(기준일: {html.escape(ref_dates[0].replace('-', '.'))}, 단위: 원)</span>"
        if ref_dates else ""
    )

    # ── 시세 표 ──
    if not latest:
        prices_section = "<p style='color:#666'>가격 데이터 없음</p>"
    else:
        dash = "<span style='color:#bbb'>-</span>"
        rows = []
        for r in latest:
            if r["change"] > 0:
                arrow, color = "▲", "#ef5350"
            elif r["change"] < 0:
                arrow, color = "▼", "#42a5f5"
            else:
                arrow, color = "─", "#666"
            s = stats.get(r["label"])
            if s:
                avg_cell = f"{s['avg']:,.0f}"
                max_cell = f"{s['max']:,.0f}"
                min_cell = f"{s['min']:,.0f}"
                range_cell = f"{s['range_pct']:.2f}%"
            else:
                avg_cell = max_cell = min_cell = range_cell = dash
            rows.append(
                "<tr>"
                f"<td style='padding:8px 10px;text-align:center'>{html.escape(r['label'])}"
                f" <span style='color:#888;font-size:12px'>"
                f"({html.escape(r['ticker'])})</span></td>"
                f"<td style='padding:8px 10px;text-align:center;font-weight:600;"
                f"font-variant-numeric:tabular-nums'>{r['close']:,.0f}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:{color};"
                f"font-variant-numeric:tabular-nums'>"
                f"{arrow} {r['change']:+,.0f} ({r['change_pct']:+.2f}%)</td>"
                f"<td style='padding:8px 10px;text-align:center;"
                f"font-variant-numeric:tabular-nums'>{avg_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#ef5350;"
                f"font-variant-numeric:tabular-nums'>{max_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#42a5f5;"
                f"font-variant-numeric:tabular-nums'>{min_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#9c27b0;"
                f"font-variant-numeric:tabular-nums'>{range_cell}</td>"
                "</tr>"
            )
        prices_section = (
            "<table style='border-collapse:collapse;width:100%;border:1px solid #ddd;font-size:14px'>"
            "<thead style='background:#f8f9fa'>"
            "<tr>"
            "<th rowspan='2' style='padding:8px 10px;text-align:center;border-bottom:1px solid #ddd'>종목</th>"
            "<th rowspan='2' style='padding:8px 10px;text-align:center;border-bottom:1px solid #ddd'>현재가</th>"
            "<th rowspan='2' style='padding:8px 10px;text-align:center;border-bottom:1px solid #ddd'>전일 대비</th>"
            f"<th rowspan='2' style='padding:8px 10px;text-align:center;border-bottom:1px solid #ddd'>"
            f"{CHART_DAYS}일 평균</th>"
            f"<th colspan='3' style='padding:8px 10px;text-align:center;border-bottom:1px solid #eee'>"
            f"{CHART_DAYS}일 변동폭</th>"
            "</tr>"
            "<tr>"
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;"
            "color:#666;border-bottom:1px solid #ddd'>최고</th>"
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;"
            "color:#666;border-bottom:1px solid #ddd'>최저</th>"
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;"
            "color:#666;border-bottom:1px solid #ddd'>%</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )

    # ── 괴리율 섹션 ──
    if discount:
        discount_section = (
            f"<h2 style='margin-top:28px;font-size:16px;color:#202124'>"
            f"📊 괴리율 "
            f"<span style='color:#888;font-size:13px;font-weight:normal'>"
            f"(보통주 대비 우선주 할인율, 단위: %)</span></h2>"
            "<table style='border-collapse:collapse;width:100%;border:1px solid #ddd;font-size:14px'>"
            "<thead style='background:#f8f9fa'>"
            "<tr>"
            "<th style='padding:8px 10px;text-align:center'>현재</th>"
            f"<th style='padding:8px 10px;text-align:center'>{CHART_DAYS}일 평균</th>"
            f"<th style='padding:8px 10px;text-align:center'>{CHART_DAYS}일 최저</th>"
            f"<th style='padding:8px 10px;text-align:center'>{CHART_DAYS}일 최고</th>"
            "</tr>"
            "</thead>"
            "<tbody><tr>"
            f"<td style='padding:8px 10px;text-align:center;font-weight:600;"
            f"font-variant-numeric:tabular-nums;color:#9c27b0'>"
            f"{discount['current']:.2f}%</td>"
            f"<td style='padding:8px 10px;text-align:center;"
            f"font-variant-numeric:tabular-nums'>{discount['avg']:.2f}%</td>"
            f"<td style='padding:8px 10px;text-align:center;color:#42a5f5;"
            f"font-variant-numeric:tabular-nums'>{discount['min']:.2f}%</td>"
            f"<td style='padding:8px 10px;text-align:center;color:#ef5350;"
            f"font-variant-numeric:tabular-nums'>{discount['max']:.2f}%</td>"
            "</tr></tbody></table>"
        )
    else:
        discount_section = ""

    # ── 차트 섹션 ──
    if has_price_image:
        price_chart_section = (
            f"<h2 style='margin-top:28px;font-size:16px;color:#202124'>"
            f"📈 {CHART_DAYS}일 추이 (주가)</h2>"
            f"{render_chart_legend(list(chart.keys()))}"
            f'<img src="cid:{PRICE_CHART_CID}" alt="{CHART_DAYS}일 주가 추이" '
            f'style="display:block;max-width:100%;height:auto;border:1px solid #eee;'
            f'border-radius:4px">'
        )
    else:
        price_chart_section = ""

    if has_discount_image:
        discount_chart_section = (
            f"<h2 style='margin-top:28px;font-size:16px;color:#202124'>"
            f"📈 {CHART_DAYS}일 추이 (괴리율)</h2>"
            f'<img src="cid:{DISCOUNT_CHART_CID}" alt="{CHART_DAYS}일 괴리율 추이" '
            f'style="display:block;max-width:100%;height:auto;border:1px solid #eee;'
            f'border-radius:4px">'
        )
    else:
        discount_chart_section = ""

    # ── 뉴스 섹션 ──
    if not news:
        news_section = "<p style='color:#666'>뉴스 없음</p>"
    else:
        news_rows = "".join(
            "<tr>"
            f"<td style='padding:6px 10px;white-space:nowrap;color:#666;font-size:12px'>"
            f"{html.escape(n.get('pub_date') or '')}</td>"
            f"<td style='padding:6px 10px'>"
            f"<a href=\"{html.escape(n['link'], quote=True)}\" "
            f"style='color:#1a73e8;text-decoration:none'>"
            f"{html.escape(n['title'])}</a></td>"
            f"<td style='padding:6px 10px;color:#444;font-size:13px'>"
            f"{html.escape(n.get('source') or '')}</td>"
            "</tr>"
            for n in news
        )
        news_section = (
            "<table style='border-collapse:collapse;width:100%;border:1px solid #ddd;font-size:14px'>"
            "<thead style='background:#f8f9fa;text-align:left'>"
            "<tr><th style='padding:8px 10px'>발행</th>"
            "<th style='padding:8px 10px'>제목</th>"
            "<th style='padding:8px 10px'>출처</th></tr></thead>"
            f"<tbody>{news_rows}</tbody></table>"
        )

    link_section = (
        "<div style='margin-top:32px;padding:14px 16px;background:#f8f9fa;"
        "border-radius:6px;text-align:center'>"
        f"<a href='{PUBLIC_REPORT_URL}' "
        "style='color:#1a73e8;text-decoration:none;font-size:14px;font-weight:500'>"
        "🔗 전체 리포트 보기</a>"
        "</div>"
    )

    return (
        "<!doctype html><html><body style=\"font-family:-apple-system,"
        "BlinkMacSystemFont,'Segoe UI','Malgun Gothic',sans-serif;"
        "max-width:900px;margin:0 auto;padding:16px;color:#202124\">"
        f"<h1 style='border-bottom:2px solid #42a5f5;padding-bottom:8px;margin-bottom:16px'>"
        f"📊 삼성전자 일일 리포트 : {today}</h1>"
        f"<h2 style='margin-top:24px;font-size:16px;color:#202124'>💹 시세{ref_caption}</h2>"
        f"{prices_section}"
        f"{discount_section}"
        f"{price_chart_section}"
        f"{discount_chart_section}"
        f"<h2 style='margin-top:28px;font-size:16px;color:#202124'>📰 관련 뉴스"
        f" <span style='color:#888;font-size:13px;font-weight:normal'>"
        f"(검색어: {NEWS_QUERY})</span></h2>"
        f"{render_summary_block_html(summary)}"
        f"{news_section}"
        f"{link_section}"
        "<p style='margin-top:24px;color:#999;font-size:12px;text-align:center'>"
        "데이터: yfinance · Google News RSS · 자동 발송</p>"
        "</body></html>"
    )


# ────────────────────────────────────────────────────────────
# 텔레그램 메시지
# ────────────────────────────────────────────────────────────

def render_telegram_message(latest: list[dict], summary: list[str] | None,
                            chart: dict, ref_date: str, today_str: str) -> str:
    """Telegram HTML: 제목 + 시세(보통/우선) + 괴리율 + AI 요약 + 링크."""
    stats = _stats_from_chart(chart)
    discount = _discount_stats(chart)

    parts = [f"📊 <b>일일현황: 삼성전자 ({today_str})</b>", ""]

    if latest:
        ref_str = (
            f" <i>(기준일: {html.escape(ref_date.replace('-', '.'))}, 단위: 원)</i>"
            if ref_date else ""
        )
        parts.append(f"💹 <b>시세</b>{ref_str}")
        for r in latest:
            arrow = "▲" if r["change"] > 0 else "▼" if r["change"] < 0 else "─"
            avg_val = stats.get(r["label"], {}).get("avg")
            avg_str = f" | 30일 평균 {avg_val:,.0f}" if avg_val is not None else ""
            parts.append(
                f"• <b>{html.escape(r['label'])}</b>: {r['close']:,.0f} "
                f"{arrow} {r['change']:+,.0f} ({r['change_pct']:+.2f}%){avg_str}"
            )
    else:
        parts.append("💹 시세: 데이터 없음")

    if discount:
        parts.extend([
            "",
            f"📊 <b>괴리율</b>: <b>{discount['current']:.2f}%</b> "
            f"<i>(30일 평균 {discount['avg']:.2f}%, "
            f"범위 {discount['min']:.2f}%~{discount['max']:.2f}%)</i>"
        ])

    if summary:
        parts.extend(["", "📰 <b>오늘의 핵심 내용</b>"])
        for s in summary:
            parts.append(f"• {html.escape(s)}")

    parts.extend([
        "",
        f'🔗 <a href="{PUBLIC_REPORT_URL}">전체 리포트 보기</a>',
    ])
    return "\n".join(parts)


# ────────────────────────────────────────────────────────────
# 콘솔 출력 & 발송 토글
# ────────────────────────────────────────────────────────────

def print_prices(rows: list[dict]) -> None:
    latest = {}
    for r in rows:
        if "error" in r:
            continue
        latest[r["ticker"]] = r
    print("\n" + "=" * 60)
    print("📊 삼성전자 시세 (최신 거래일)")
    print("=" * 60)
    for ticker, r in latest.items():
        arrow = "▲" if r["change"] > 0 else ("▼" if r["change"] < 0 else "─")
        print(
            f"  {r['label']:<8} ({ticker:<10}) {r['date']}  "
            f"{r['close']:>10,.0f}  {arrow} {r['change']:+,.0f} "
            f"({r['change_pct']:+.2f}%)"
        )


def print_news(news: list[dict]) -> None:
    print("\n" + "=" * 60)
    print(f"📰 관련 뉴스 상위 {len(news)}건  (검색어: {NEWS_QUERY})")
    print("=" * 60)
    for i, n in enumerate(news, 1):
        print(f"\n[{i}] {n['title']}")
        print(f"    출처: {n['source']}  |  {n['pub_date']}")


def save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _should_send_email() -> bool:
    if "--email" in sys.argv:
        return True
    return os.environ.get("SEND_EMAIL", "").strip().lower() in ("1", "true", "yes")


def _should_send_telegram() -> bool:
    if "--telegram" in sys.argv:
        return True
    return os.environ.get("SEND_TELEGRAM", "").strip().lower() in ("1", "true", "yes")


# ────────────────────────────────────────────────────────────
# 메인
# ────────────────────────────────────────────────────────────

def main() -> int:
    # 멱등성: cron 슬롯이 여러 개라 같은 날 중복 트리거될 수 있다.
    if already_sent_today_kst():
        print("✓ 오늘(KST) 이미 발송 완료 — 종료", file=sys.stderr)
        return 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)

        try:
            prices = fetch_prices()
            print_prices(prices)
            upsert_prices(conn, prices, captured_at)
        except Exception as e:
            print(f"[오류] 시세 조회 실패: {e}", file=sys.stderr)
            prices = []

        try:
            news = fetch_news(NEWS_QUERY, NEWS_LIMIT)
            print_news(news)
            upsert_news(conn, news, captured_at)
        except Exception as e:
            print(f"[오류] 뉴스 조회 실패: {e}", file=sys.stderr)
            news = []

        # 공통 데이터 (이메일·텔레그램·GH Pages 가 같은 스냅샷 사용)
        latest = load_latest_prices(conn)
        chart = load_history_for_chart(conn, CHART_DAYS)
        recent_news = load_recent_news(conn, NEWS_LIMIT)
        summary = summarize_titles(
            [n["title"] for n in recent_news],
            domain="삼성전자 시황",
            concrete_examples=(
                "구체적 가격대·종목명·이슈명 "
                "(예: '보통주 75,000원 돌파', '괴리율 20% 확대', "
                "'실적 발표', '외국인 순매수')"
            ),
        )
        today = datetime.now(KST).strftime("%Y.%m.%d")
        ref_date = max((r["date"] for r in latest if r.get("date")), default="")

        # 차트 PNG 생성
        price_png = render_price_chart_png(chart)
        discount_png = render_discount_chart_png(chart)

        # 이메일 본문 HTML
        email_html = render_email_html(
            latest, recent_news, chart, summary,
            has_price_image=bool(price_png),
            has_discount_image=bool(discount_png),
        )

        # GH Pages 사본 — cid: 를 상대 경로로 치환, PNG 파일도 함께 저장
        try:
            PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
            public_html = email_html.replace(
                f"cid:{PRICE_CHART_CID}", f"{PRICE_CHART_CID}.png"
            ).replace(
                f"cid:{DISCOUNT_CHART_CID}", f"{DISCOUNT_CHART_CID}.png"
            )
            PUBLIC_REPORT_PATH.write_text(public_html, encoding="utf-8")
            if price_png:
                (PUBLIC_DIR / f"{PRICE_CHART_CID}.png").write_bytes(price_png)
            if discount_png:
                (PUBLIC_DIR / f"{DISCOUNT_CHART_CID}.png").write_bytes(discount_png)
            print(f"\n🌐 공개본: {PUBLIC_REPORT_PATH}")
        except Exception as e:
            print(f"[오류] 공개본 저장 실패: {e}", file=sys.stderr)

        # 이메일
        if _should_send_email():
            try:
                inline: dict[str, bytes] = {}
                if price_png:
                    inline[PRICE_CHART_CID] = price_png
                if discount_png:
                    inline[DISCOUNT_CHART_CID] = discount_png
                send_html_email(
                    f"일일현황: 삼성전자 ({today})",
                    email_html,
                    inline_images=inline or None,
                )
                tags = []
                if summary:
                    tags.append("AI 요약")
                if price_png:
                    tags.append("주가 차트")
                if discount_png:
                    tags.append("괴리율 차트")
                print("📧 이메일 발송 완료" + (f" ({' + '.join(tags)})" if tags else ""))
            except KeyError as e:
                print(f"[오류] 환경변수 누락: {e} "
                      "(GMAIL_USER, GMAIL_APP_PASSWORD 필요)", file=sys.stderr)
                return 1
            except Exception as e:
                print(f"[오류] 이메일 발송 실패: {e}", file=sys.stderr)
                return 1

        # 텔레그램
        if _should_send_telegram():
            try:
                msg = render_telegram_message(latest, summary, chart, ref_date, today)
                send_telegram_message(msg)
                print("💬 텔레그램 발송 완료")
            except KeyError as e:
                print(f"[오류] 환경변수 누락: {e} "
                      "(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 필요)", file=sys.stderr)
                return 1
            except Exception as e:
                print(f"[오류] 텔레그램 발송 실패: {e}", file=sys.stderr)
                return 1

        return 0 if (prices or news) else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
