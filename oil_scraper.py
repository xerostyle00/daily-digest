"""국제유가 시세 + 관련 뉴스 스크래퍼 (히스토리 관리 + HTML 리포트).

- 유가: yfinance 로 WTI(CL=F), Brent(BZ=F), 천연가스(NG=F) 일별 시세 조회
- 뉴스: Google News RSS 에서 "국제유가" 관련 상위 10건 수집
- 저장:
    1) SQLite DB (output/history.db) — 날짜별 누적 (UPSERT 로 중복 방지)
    2) CSV 스냅샷 (output/prices_*.csv, news_*.csv) — 실행 시점별 백업
    3) HTML 리포트 (output/report.html) — Chart.js 로 최근 30일 추이 시각화
"""

from __future__ import annotations

import csv
import html
import io
import json
import os
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import matplotlib
matplotlib.use("Agg")  # 헤드리스 렌더링 (GH Actions 환경)
import matplotlib.pyplot as plt  # noqa: E402
import requests  # noqa: E402
import yfinance as yf  # noqa: E402

from mailer import send_html_email  # noqa: E402
from notifier import send_telegram_message  # noqa: E402
from summarizer import render_summary_block_html, summarize_titles  # noqa: E402

# Windows 콘솔 한글/이모지 출력을 위한 UTF-8 재설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

KST = timezone(timedelta(hours=9))
OUTPUT_DIR = Path(__file__).parent / "output"
DB_PATH = OUTPUT_DIR / "OIL_history.db"
REPORT_PATH = OUTPUT_DIR / "OIL_report.html"
# GitHub Pages 호스팅용 사본 (output/public/oil/index.html → /daily-digest/oil/)
PUBLIC_REPORT_PATH = OUTPUT_DIR / "public" / "oil" / "index.html"
PUBLIC_REPORT_URL = "https://xerostyle00.github.io/daily-digest/oil/"
CHART_IMAGE_CID = "chart_combined"
NEWS_QUERY = "국제유가"
NEWS_LIMIT = 10
CHART_DAYS = 30
GOOGLE_NEWS_RSS = (
    "https://news.google.com/rss/search"
    "?q={query}&hl=ko&gl=KR&ceid=KR:ko"
)
TICKERS = {
    "WTI 원유": "CL=F",
    "Brent 원유": "BZ=F",
    "천연가스": "NG=F",
}
TICKER_COLORS = {
    "WTI 원유": "#ef5350",
    "Brent 원유": "#42a5f5",
    "천연가스": "#ffb74d",
}
# ────────────────────────────────────────────────────────────
# 데이터 수집
# ────────────────────────────────────────────────────────────

def fetch_prices() -> list[dict]:
    """최근 거래일 종가와 전일 대비 등락. CHART_DAYS 만큼의 이력도 함께 반환."""
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
            "title": title,
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


def load_history_for_chart(conn: sqlite3.Connection, days: int) -> dict:
    """Chart.js 용 데이터: {label: [{date, close}, ...]} 형태."""
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


def export_monthly_csv(conn: sqlite3.Connection, year_month: str) -> tuple[Path, Path]:
    """해당 월(YYYY-MM)의 prices/news 를 각각 단일 CSV 로 덮어써 내보낸다.

    - prices: 거래일(date) 기준 필터
    - news:   수집 시각(captured_at) 기준 필터
    중복은 DB 단계에서 이미 제거되어 있으므로 단순 SELECT → 덮어쓰기.
    """
    prices_path = OUTPUT_DIR / f"OIL_prices_{year_month.replace('-', '')}.csv"
    news_path = OUTPUT_DIR / f"OIL_news_{year_month.replace('-', '')}.csv"

    price_cursor = conn.execute(
        """
        SELECT label, ticker, date, close, prev_close, change, change_pct, captured_at
        FROM prices
        WHERE substr(date, 1, 7) = ?
        ORDER BY date, ticker
        """,
        (year_month,),
    )
    price_rows = [
        {
            "label": r[0], "ticker": r[1], "date": r[2], "close": r[3],
            "prev_close": r[4], "change": r[5], "change_pct": r[6], "captured_at": r[7],
        }
        for r in price_cursor.fetchall()
    ]
    save_csv(prices_path, price_rows)

    news_cursor = conn.execute(
        """
        SELECT title, source, pub_date, link, query, captured_at
        FROM news
        WHERE substr(captured_at, 1, 7) = ?
        ORDER BY captured_at DESC, pub_date DESC
        """,
        (year_month,),
    )
    news_rows = [
        {
            "title": r[0], "source": r[1], "pub_date": r[2],
            "link": r[3], "query": r[4], "captured_at": r[5],
        }
        for r in news_cursor.fetchall()
    ]
    save_csv(news_path, news_rows)

    return prices_path, news_path


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


def load_monthly_stats(conn: sqlite3.Connection) -> dict:
    """{label: [{month, min, max, avg, count}, ...]} — 월별 집계."""
    result: dict[str, list[dict]] = {}
    for label, ticker in TICKERS.items():
        cursor = conn.execute(
            """
            SELECT substr(date, 1, 7) AS ym,
                   MIN(close), MAX(close), AVG(close), COUNT(*)
            FROM prices
            WHERE ticker = ?
            GROUP BY ym
            ORDER BY ym
            """,
            (ticker,),
        )
        result[label] = [
            {
                "month": r[0],
                "min": round(r[1], 2),
                "max": round(r[2], 2),
                "avg": round(r[3], 2),
                "count": r[4],
            }
            for r in cursor.fetchall()
        ]
    return result


# ────────────────────────────────────────────────────────────
# HTML 리포트
# ────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>국제유가 리포트</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1115; --card: #181b22; --border: #2a2f3a;
    --text: #e8eaed; --muted: #9aa0a6; --up: #ef5350; --down: #42a5f5;
    --accent: #ffb74d;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 24px; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Malgun Gothic",
                 "Apple SD Gothic Neo", sans-serif;
    line-height: 1.5;
  }}
  .container {{ max-width: 1100px; margin: 0 auto; }}
  header {{ display: flex; justify-content: space-between; align-items: baseline;
           margin-bottom: 24px; flex-wrap: wrap; gap: 8px; }}
  h1 {{ margin: 0; font-size: 24px; font-weight: 600; }}
  .updated {{ color: var(--muted); font-size: 13px; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
           gap: 16px; margin-bottom: 24px; }}
  .card {{ background: var(--card); border: 1px solid var(--border);
          border-radius: 12px; padding: 18px 20px; }}
  .card .label {{ color: var(--muted); font-size: 13px; margin-bottom: 6px; }}
  .card .price {{ font-size: 28px; font-weight: 600; }}
  .card .change {{ margin-top: 4px; font-size: 14px; }}
  .card .date {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
  .up {{ color: var(--up); }} .down {{ color: var(--down); }} .flat {{ color: var(--muted); }}
  .section {{ background: var(--card); border: 1px solid var(--border);
             border-radius: 12px; padding: 20px; margin-bottom: 24px; }}
  .section h2 {{ margin: 0 0 16px; font-size: 16px; font-weight: 600; color: var(--accent); }}
  .chart-wrap {{ position: relative; height: 360px; }}
  .news-list {{ list-style: none; padding: 0; margin: 0; }}
  .news-list li {{ padding: 12px 0; border-bottom: 1px solid var(--border); }}
  .news-list li:last-child {{ border-bottom: none; }}
  .news-list a {{ color: var(--text); text-decoration: none; font-weight: 500; }}
  .news-list a:hover {{ color: var(--accent); text-decoration: underline; }}
  .news-meta {{ color: var(--muted); font-size: 12px; margin-top: 4px; }}
  footer {{ color: var(--muted); font-size: 12px; text-align: center; margin-top: 24px; }}

  /* 탭 */
  .tabs {{ display: flex; gap: 4px; margin-bottom: 16px;
          border-bottom: 1px solid var(--border); }}
  .tab {{ background: none; color: var(--muted); border: none; padding: 10px 18px;
         cursor: pointer; border-bottom: 2px solid transparent; font-size: 14px;
         font-family: inherit; }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ color: var(--accent); border-bottom-color: var(--accent); }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}

  /* 월별 차트용 종목 선택 */
  .ticker-selector {{ display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }}
  .ticker-btn {{ background: var(--bg); color: var(--muted); border: 1px solid var(--border);
                border-radius: 6px; padding: 6px 14px; cursor: pointer; font-size: 13px;
                font-family: inherit; }}
  .ticker-btn:hover {{ color: var(--text); }}
  .ticker-btn.active {{ background: var(--accent); color: #000; border-color: var(--accent); }}

  /* 월별 요약 테이블 */
  .monthly-table {{ width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 13px; }}
  .monthly-table th, .monthly-table td {{ padding: 8px 12px;
                                         border-bottom: 1px solid var(--border); }}
  .monthly-table th {{ color: var(--muted); font-weight: 500; text-align: right;
                      background: rgba(255,255,255,0.02); }}
  .monthly-table td {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .monthly-table th:nth-child(1), .monthly-table th:nth-child(2),
  .monthly-table td:nth-child(1), .monthly-table td:nth-child(2) {{ text-align: left; }}
  .monthly-table tbody tr:hover {{ background: rgba(255,255,255,0.03); }}

  /* 종목 색상 배지 (차트 색과 동일) */
  .ticker-pill {{ display: inline-block; padding: 2px 10px; border-radius: 10px;
                 font-size: 12px; font-weight: 600; letter-spacing: 0.2px; }}
  .ticker-pill-0 {{ background: #ef5350; color: #fff; }}   /* WTI */
  .ticker-pill-1 {{ background: #42a5f5; color: #fff; }}   /* Brent */
  .ticker-pill-2 {{ background: #ffb74d; color: #1a1a1a; }} /* 천연가스 */

  /* 최저/평균/최고 색상 */
  .col-min {{ color: #64b5f6; font-weight: 600; }}   /* 저 → 파랑 */
  .col-avg {{ color: #ffd54f; font-weight: 600; }}   /* 평균 → 노랑 */
  .col-max {{ color: #ef5350; font-weight: 600; }}   /* 고 → 빨강 */
  .col-range {{ color: #ba68c8; }}                    /* 변동폭 → 보라 */
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>📈 국제유가 리포트</h1>
    <div class="updated">업데이트: {updated}</div>
  </header>

  <div class="cards">{cards}</div>

  <div class="section">
    <div class="tabs">
      <button class="tab active" data-tab="daily">일별 차트</button>
      <button class="tab" data-tab="monthly">월별 차트</button>
    </div>

    <div class="tab-content active" id="tab-daily">
      <h2>최근 {days}일 추이</h2>
      <div class="chart-wrap"><canvas id="priceChart"></canvas></div>
    </div>

    <div class="tab-content" id="tab-monthly">
      <h2>월별 최고/최저/평균</h2>
      <div class="ticker-selector">{ticker_buttons}</div>
      <div class="chart-wrap"><canvas id="monthlyChart"></canvas></div>
      <table class="monthly-table">
        <thead><tr>
          <th>월</th><th>종목</th>
          <th>최저</th><th>평균</th><th>최고</th>
          <th>변동폭</th><th>거래일</th>
        </tr></thead>
        <tbody>{monthly_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>📰 관련 뉴스</h2>
    <ul class="news-list">{news_items}</ul>
  </div>

  <footer>데이터: yfinance · Google News RSS &nbsp;|&nbsp; 자동 생성</footer>
</div>

<script>
const chartData = {chart_data};
const monthlyData = {monthly_data};
const colors = ['#ef5350', '#42a5f5', '#ffb74d', '#66bb6a', '#ab47bc'];
const tickerColorMap = {{}};
Object.keys(chartData).forEach((label, i) => {{
  tickerColorMap[label] = colors[i % colors.length];
}});

// ── 일별 차트 ──
const dailyDatasets = Object.keys(chartData).map((label) => ({{
  label: label,
  data: chartData[label].map(p => ({{x: p.date, y: p.close}})),
  borderColor: tickerColorMap[label],
  backgroundColor: tickerColorMap[label] + '22',
  tension: 0.25,
  pointRadius: 2,
  borderWidth: 2,
}}));

new Chart(document.getElementById('priceChart'), {{
  type: 'line',
  data: {{ datasets: dailyDatasets }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{
      x: {{ type: 'category', ticks: {{ color: '#9aa0a6', maxRotation: 0 }},
           grid: {{ color: '#2a2f3a' }} }},
      y: {{ ticks: {{ color: '#9aa0a6' }}, grid: {{ color: '#2a2f3a' }} }}
    }},
    plugins: {{
      legend: {{ labels: {{ color: '#e8eaed' }} }},
      tooltip: {{ backgroundColor: '#181b22', borderColor: '#2a2f3a', borderWidth: 1 }}
    }}
  }}
}});

// ── 월별 차트 ──
let monthlyChart = null;

function renderMonthlyChart(label) {{
  const series = monthlyData[label] || [];
  const color = tickerColorMap[label] || '#ffb74d';
  const labels = series.map(s => s.month);
  const rangeData = series.map(s => [s.min, s.max]);
  const avgData = series.map(s => s.avg);

  if (monthlyChart) monthlyChart.destroy();
  monthlyChart = new Chart(document.getElementById('monthlyChart'), {{
    data: {{
      labels: labels,
      datasets: [
        {{
          type: 'bar', label: '최저~최고 범위', data: rangeData,
          backgroundColor: color + '55', borderColor: color, borderWidth: 1,
          borderSkipped: false, barPercentage: 0.5,
        }},
        {{
          type: 'line', label: '평균', data: avgData,
          borderColor: color, backgroundColor: color,
          pointRadius: 6, pointHoverRadius: 8, pointStyle: 'rectRot',
          showLine: false,
        }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      scales: {{
        x: {{ ticks: {{ color: '#9aa0a6' }}, grid: {{ color: '#2a2f3a' }} }},
        y: {{ ticks: {{ color: '#9aa0a6' }}, grid: {{ color: '#2a2f3a' }} }}
      }},
      plugins: {{
        legend: {{ labels: {{ color: '#e8eaed' }} }},
        tooltip: {{
          backgroundColor: '#181b22', borderColor: '#2a2f3a', borderWidth: 1,
          callbacks: {{
            label: function(ctx) {{
              if (ctx.dataset.type === 'bar') {{
                const [lo, hi] = ctx.raw;
                return `범위: ${{lo.toFixed(2)}} ~ ${{hi.toFixed(2)}} (변동 ${{(hi-lo).toFixed(2)}})`;
              }}
              return `평균: ${{ctx.raw.toFixed(2)}}`;
            }}
          }}
        }}
      }}
    }}
  }});
}}

// 탭 전환
document.querySelectorAll('.tab').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  }});
}});

// 종목 선택
document.querySelectorAll('.ticker-btn').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.ticker-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    renderMonthlyChart(btn.dataset.ticker);
  }});
}});

// 초기 월별 차트
const firstTicker = document.querySelector('.ticker-btn.active');
if (firstTicker) renderMonthlyChart(firstTicker.dataset.ticker);
</script>
</body>
</html>
"""


def render_card(row: dict) -> str:
    if row["change"] > 0:
        cls, arrow = "up", "▲"
    elif row["change"] < 0:
        cls, arrow = "down", "▼"
    else:
        cls, arrow = "flat", "─"
    return (
        f'<div class="card">'
        f'<div class="label">{html.escape(row["label"])} ({html.escape(row["ticker"])})</div>'
        f'<div class="price">{row["close"]:,.2f}</div>'
        f'<div class="change {cls}">{arrow} {row["change"]:+.2f} ({row["change_pct"]:+.2f}%)</div>'
        f'<div class="date">{html.escape(row["date"])}</div>'
        f"</div>"
    )


def render_news_item(n: dict) -> str:
    return (
        f"<li>"
        f'<a href="{html.escape(n["link"])}" target="_blank" rel="noopener">'
        f'{html.escape(n["title"])}</a>'
        f'<div class="news-meta">{html.escape(n.get("source") or "")}'
        f' · {html.escape(n.get("pub_date") or "")}</div>'
        f"</li>"
    )


def render_ticker_buttons(labels: list[str]) -> str:
    return "".join(
        f'<button class="ticker-btn {"active" if i == 0 else ""}" '
        f'data-ticker="{html.escape(label)}">{html.escape(label)}</button>'
        for i, label in enumerate(labels)
    )


def render_monthly_rows(monthly_stats: dict) -> str:
    ticker_order = {label: i for i, label in enumerate(TICKERS)}
    flat = [
        {"label": label, **row}
        for label, series in monthly_stats.items()
        for row in series
    ]
    flat.sort(key=lambda r: (-int(r["month"].replace("-", "")), ticker_order.get(r["label"], 99)))
    rows = "".join(
        f"<tr>"
        f"<td>{html.escape(r['month'])}</td>"
        f'<td><span class="ticker-pill ticker-pill-{ticker_order[r["label"]]}">'
        f'{html.escape(r["label"])}</span></td>'
        f'<td class="col-min">{r["min"]:,.2f}</td>'
        f'<td class="col-avg">{r["avg"]:,.2f}</td>'
        f'<td class="col-max">{r["max"]:,.2f}</td>'
        f'<td class="col-range">{(r["max"] - r["min"]):,.2f}</td>'
        f"<td>{r['count']}</td>"
        f"</tr>"
        for r in flat
    )
    return rows or "<tr><td colspan='7' style='text-align:center;color:var(--muted);'>데이터 없음</td></tr>"


def render_html(conn: sqlite3.Connection) -> str:
    latest = load_latest_prices(conn)
    chart = load_history_for_chart(conn, CHART_DAYS)
    news = load_recent_news(conn, NEWS_LIMIT)
    monthly = load_monthly_stats(conn)
    return HTML_TEMPLATE.format(
        updated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        cards="".join(render_card(r) for r in latest) or "<div class='card'>데이터 없음</div>",
        days=CHART_DAYS,
        chart_data=json.dumps(chart, ensure_ascii=False),
        monthly_data=json.dumps(monthly, ensure_ascii=False),
        ticker_buttons=render_ticker_buttons(list(TICKERS.keys())),
        monthly_rows=render_monthly_rows(monthly),
        news_items="".join(render_news_item(n) for n in news) or "<li>뉴스 없음</li>",
    )


# ────────────────────────────────────────────────────────────
# 이메일 발송 (Gmail SMTP)
# ────────────────────────────────────────────────────────────

def _stats_from_chart(chart: dict) -> dict[str, dict]:
    """{label: {min, max, avg, range_pct}} — 시리즈 전체에서 계산."""
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


CHART_Y_MIN = 50.0
CHART_Y_MAX = 150.0


def _chart_eligible_labels(chart: dict) -> list[str]:
    """차트 Y범위 [CHART_Y_MIN, CHART_Y_MAX] 안에 들어오는 종목만 반환 (median 기준).

    가격대가 다른 종목(예: 천연가스 ~$3)을 한 차트에 그리면 평선처럼 보이므로 제외.
    """
    out: list[str] = []
    for label, series in chart.items():
        if len(series) < 2 or not series[0]["close"]:
            continue
        closes = sorted(p["close"] for p in series)
        median = closes[len(closes) // 2]
        if CHART_Y_MIN - 10 <= median <= CHART_Y_MAX + 10:
            out.append(label)
    return out


def render_combined_chart_png(chart: dict) -> bytes:
    """30일 추이 PNG (실제 종가, Y축 $50~$150 고정). 데이터 부족 시 빈 bytes."""
    labels = _chart_eligible_labels(chart)
    if not labels:
        return b""

    series_any = chart[labels[0]]
    max_dates = [p["date"] for p in series_any]
    for label in labels[1:]:
        if len(chart[label]) > len(max_dates):
            max_dates = [p["date"] for p in chart[label]]

    fig, ax = plt.subplots(figsize=(8.2, 2.6), dpi=140)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for label in labels:
        series = chart[label]
        x = list(range(len(series)))
        y = [p["close"] for p in series]
        # 한국어 라벨은 PNG 안에 넣지 않음 (HTML 범례에서 표시).
        ax.plot(x, y, color=TICKER_COLORS.get(label, "#666"),
                linewidth=2.0, solid_capstyle="round")

    ax.set_ylim(CHART_Y_MIN, CHART_Y_MAX)
    ax.set_yticks([CHART_Y_MIN, 75, 100, 125, CHART_Y_MAX])

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


def render_chart_legend(labels: list[str]) -> str:
    items = "".join(
        f'<span style="display:inline-block;margin-right:16px">'
        f'<span style="display:inline-block;width:10px;height:10px;background:{TICKER_COLORS.get(label, "#666")};'
        f'vertical-align:middle;margin-right:6px"></span>'
        f'<span style="vertical-align:middle;font-size:13px;color:#444">{html.escape(label)}</span>'
        f"</span>"
        for label in labels
    )
    return (
        '<div style="margin:12px 0 4px 0">'
        f"{items}"
        '<span style="color:#999;font-size:11px;margin-left:8px">'
        "(일별 종가, 단위: $)</span>"
        "</div>"
    )


def render_email_html(latest: list[dict], news: list[dict], chart: dict,
                      summary: list[str] | None = None,
                      *, has_chart_image: bool = False) -> str:
    """이메일 클라이언트 호환 HTML (인라인 스타일, 표 기반, JS 없음).

    has_chart_image=True 일 때 차트는 <img src="cid:..."> 로 참조 (PNG 인라인 첨부).
    False 면 차트 섹션 자체를 생략.
    """
    today = datetime.now(KST).strftime("%Y.%m.%d (%a)")
    stats = _stats_from_chart(chart)

    ref_dates = sorted({r["date"] for r in latest if r.get("date")}, reverse=True)
    ref_caption = (
        f" <span style='color:#888;font-size:13px;font-weight:normal'>"
        f"(기준일: {html.escape(ref_dates[0].replace('-', '.'))}, 단위: $)</span>"
        if ref_dates else ""
    )

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
                avg_cell = f"{s['avg']:,.2f}"
                max_cell = f"{s['max']:,.2f}"
                min_cell = f"{s['min']:,.2f}"
                range_cell = f"{s['range_pct']:.2f}%"
            else:
                avg_cell = max_cell = min_cell = range_cell = dash
            rows.append(
                "<tr>"
                f"<td style='padding:8px 10px;text-align:center'>{html.escape(r['label'])}"
                f" <span style='color:#888;font-size:12px'>({html.escape(r['ticker'])})</span></td>"
                f"<td style='padding:8px 10px;text-align:center;font-weight:600;font-variant-numeric:tabular-nums'>"
                f"{r['close']:,.2f}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:{color};font-variant-numeric:tabular-nums'>"
                f"{arrow} {r['change']:+.2f} ({r['change_pct']:+.2f}%)</td>"
                f"<td style='padding:8px 10px;text-align:center;font-variant-numeric:tabular-nums'>"
                f"{avg_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#ef5350;font-variant-numeric:tabular-nums'>"
                f"{max_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#42a5f5;font-variant-numeric:tabular-nums'>"
                f"{min_cell}</td>"
                f"<td style='padding:8px 10px;text-align:center;color:#9c27b0;font-variant-numeric:tabular-nums'>"
                f"{range_cell}</td>"
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
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;color:#666;"
            "border-bottom:1px solid #ddd'>최고</th>"
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;color:#666;"
            "border-bottom:1px solid #ddd'>최저</th>"
            "<th style='padding:6px 10px;text-align:center;font-weight:500;font-size:12px;color:#666;"
            "border-bottom:1px solid #ddd'>%</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )

    if not news:
        news_section = "<p style='color:#666'>뉴스 없음</p>"
    else:
        news_rows = "".join(
            "<tr>"
            f"<td style='padding:6px 10px;white-space:nowrap;color:#666;font-size:12px'>"
            f"{html.escape(n.get('pub_date') or '')}</td>"
            f"<td style='padding:6px 10px'><a href=\"{html.escape(n['link'], quote=True)}\" "
            f"style='color:#1a73e8;text-decoration:none'>{html.escape(n['title'])}</a></td>"
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

    if has_chart_image:
        chart_section = (
            f"<h2 style='margin-top:28px;font-size:16px;color:#202124'>"
            f"📈 {CHART_DAYS}일 추이</h2>"
            f"{render_chart_legend(_chart_eligible_labels(chart))}"
            f'<img src="cid:{CHART_IMAGE_CID}" alt="{CHART_DAYS}일 추이" '
            f'style="display:block;max-width:100%;height:auto;border:1px solid #eee;'
            f'border-radius:4px">'
        )
    else:
        chart_section = ""

    link_section = (
        "<div style='margin-top:32px;padding:14px 16px;background:#f8f9fa;"
        "border-radius:6px;text-align:center'>"
        f"<a href='{PUBLIC_REPORT_URL}' "
        "style='color:#1a73e8;text-decoration:none;font-size:14px;font-weight:500'>"
        "🔗 전체 리포트 보기 (인터랙티브 차트 + 월별 통계)</a>"
        "</div>"
    )

    return (
        "<!doctype html><html><body style=\"font-family:-apple-system,"
        "BlinkMacSystemFont,'Segoe UI','Malgun Gothic',sans-serif;"
        "max-width:900px;margin:0 auto;padding:16px;color:#202124\">"
        f"<h1 style='border-bottom:2px solid #ffb74d;padding-bottom:8px;margin-bottom:16px'>"
        f"📈 국제유가 일일 리포트 : {today}</h1>"
        f"<h2 style='margin-top:24px;font-size:16px;color:#202124'>💹 시세{ref_caption}</h2>"
        f"{prices_section}"
        f"{chart_section}"
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


def render_telegram_message(latest: list[dict], summary: list[str] | None,
                            chart: dict, ref_date: str, today_str: str) -> str:
    """Telegram HTML 메시지: 제목 + 시세 요약 + AI 요약 불릿 + 리포트 링크."""
    stats = _stats_from_chart(chart)

    parts = [f"📈 <b>일일 OIL 현황 ({today_str})</b>", ""]

    if latest:
        ref_str = (
            f" <i>(기준일: {html.escape(ref_date.replace('-', '.'))}, 단위: $)</i>"
            if ref_date else ""
        )
        parts.append(f"💹 <b>시세</b>{ref_str}")
        for r in latest:
            arrow = "▲" if r["change"] > 0 else "▼" if r["change"] < 0 else "─"
            avg_val = stats.get(r["label"], {}).get("avg")
            avg_str = f" | 30일 평균 {avg_val:,.2f}" if avg_val is not None else ""
            parts.append(
                f"• <b>{html.escape(r['label'])}</b>: {r['close']:,.2f} "
                f"{arrow} {r['change']:+.2f} ({r['change_pct']:+.2f}%){avg_str}"
            )
    else:
        parts.append("💹 시세: 데이터 없음")

    if summary:
        parts.extend(["", "📰 <b>오늘의 핵심 내용</b>"])
        for s in summary:
            parts.append(f"• {html.escape(s)}")

    parts.extend([
        "",
        f'🔗 <a href="{PUBLIC_REPORT_URL}">전체 리포트 보기</a>',
    ])
    return "\n".join(parts)


def _should_send_telegram() -> bool:
    if "--telegram" in sys.argv:
        return True
    return os.environ.get("SEND_TELEGRAM", "").strip().lower() in ("1", "true", "yes")


def _should_send_email() -> bool:
    if "--email" in sys.argv:
        return True
    return os.environ.get("SEND_EMAIL", "").strip().lower() in ("1", "true", "yes")


# ────────────────────────────────────────────────────────────
# 콘솔 출력 & CSV 백업
# ────────────────────────────────────────────────────────────

def print_prices(rows: list[dict]) -> None:
    latest = {}
    for r in rows:
        if "error" in r:
            continue
        latest[r["ticker"]] = r
    print("\n" + "=" * 60)
    print("📈 국제유가 시세 (최신 거래일)")
    print("=" * 60)
    for ticker, r in latest.items():
        arrow = "▲" if r["change"] > 0 else ("▼" if r["change"] < 0 else "─")
        print(
            f"  {r['label']:<12} ({ticker:<6}) {r['date']}  "
            f"{r['close']:>8.2f}  {arrow} {r['change']:+.2f} ({r['change_pct']:+.2f}%)"
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


# ────────────────────────────────────────────────────────────
# 메인
# ────────────────────────────────────────────────────────────

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    now_local = datetime.now()
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    year_month = now_local.strftime("%Y-%m")

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)

        try:
            prices = fetch_prices()
            print_prices(prices)
            upsert_prices(conn, prices, captured_at)
        except Exception as e:
            print(f"[오류] 유가 조회 실패: {e}", file=sys.stderr)
            prices = []

        try:
            news = fetch_news(NEWS_QUERY, NEWS_LIMIT)
            print_news(news)
            upsert_news(conn, news, captured_at)
        except Exception as e:
            print(f"[오류] 뉴스 조회 실패: {e}", file=sys.stderr)
            news = []

        # 월간 CSV 내보내기 (DB 에서 이번 달 데이터를 덮어쓰기)
        try:
            prices_csv, news_csv = export_monthly_csv(conn, year_month)
        except Exception as e:
            print(f"[오류] 월간 CSV 내보내기 실패: {e}", file=sys.stderr)
            prices_csv = news_csv = None

        # HTML 리포트 생성 (DB 전체 히스토리 기준).
        # 같은 내용을 GitHub Pages 호스팅 경로에도 복사 — Telegram/이메일 링크 대상.
        try:
            report_html = render_html(conn)
            REPORT_PATH.write_text(report_html, encoding="utf-8")
            PUBLIC_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
            PUBLIC_REPORT_PATH.write_text(report_html, encoding="utf-8")
            print("\n" + "=" * 60)
            print(f"📄 리포트:   {REPORT_PATH}")
            print(f"🌐 공개본:   {PUBLIC_REPORT_PATH}")
            print(f"💾 DB:       {DB_PATH}")
            if prices_csv and news_csv:
                print(f"📊 월간 CSV: {prices_csv.name}, {news_csv.name}")
            print("=" * 60)
        except Exception as e:
            print(f"[오류] HTML 생성 실패: {e}", file=sys.stderr)
            return 1

        # 발송 공통 데이터 (이메일·텔레그램이 같은 스냅샷 사용)
        latest = load_latest_prices(conn)
        chart = load_history_for_chart(conn, CHART_DAYS)
        recent_news = load_recent_news(conn, NEWS_LIMIT)
        summary = summarize_titles(
            [n["title"] for n in recent_news],
            domain="국제유가",
            concrete_examples=(
                "구체적 숫자·가격대·종목명 "
                "(예: 'WTI 100달러 돌파', '에쓰오일 8% 상승')"
            ),
        )
        today = datetime.now(KST).strftime("%Y.%m.%d")
        ref_date = max((r["date"] for r in latest if r.get("date")), default="")

        if _should_send_email():
            try:
                chart_png = render_combined_chart_png(chart)
                inline = {CHART_IMAGE_CID: chart_png} if chart_png else None
                html_body = render_email_html(
                    latest, recent_news, chart, summary,
                    has_chart_image=bool(chart_png),
                )
                send_html_email(
                    f"일일 OIL 현황 ({today})",
                    html_body,
                    inline_images=inline,
                )
                print("📧 이메일 발송 완료"
                      + (" (AI 요약 포함)" if summary else "")
                      + (" + 차트 PNG" if chart_png else ""))
            except KeyError as e:
                print(f"[오류] 환경변수 누락: {e} (GMAIL_USER, GMAIL_APP_PASSWORD 필요)",
                      file=sys.stderr)
                return 1
            except Exception as e:
                print(f"[오류] 이메일 발송 실패: {e}", file=sys.stderr)
                return 1

        if _should_send_telegram():
            try:
                msg = render_telegram_message(latest, summary, chart, ref_date, today)
                send_telegram_message(msg)
                print("💬 텔레그램 발송 완료")
            except KeyError as e:
                print(f"[오류] 환경변수 누락: {e} "
                      "(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 필요)",
                      file=sys.stderr)
                return 1
            except Exception as e:
                print(f"[오류] 텔레그램 발송 실패: {e}", file=sys.stderr)
                return 1

        return 0 if (prices or news) else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
