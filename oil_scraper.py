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
import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
import yfinance as yf

# Windows 콘솔 한글/이모지 출력을 위한 UTF-8 재설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

OUTPUT_DIR = Path(__file__).parent / "output"
DB_PATH = OUTPUT_DIR / "OIL_history.db"
REPORT_PATH = OUTPUT_DIR / "OIL_report.html"
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

        # HTML 리포트 생성 (DB 전체 히스토리 기준)
        try:
            REPORT_PATH.write_text(render_html(conn), encoding="utf-8")
            print("\n" + "=" * 60)
            print(f"📄 리포트:   {REPORT_PATH}")
            print(f"💾 DB:       {DB_PATH}")
            if prices_csv and news_csv:
                print(f"📊 월간 CSV: {prices_csv.name}, {news_csv.name}")
            print("=" * 60)
        except Exception as e:
            print(f"[오류] HTML 생성 실패: {e}", file=sys.stderr)
            return 1

        return 0 if (prices or news) else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
