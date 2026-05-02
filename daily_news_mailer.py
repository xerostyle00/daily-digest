"""Daily AI / GPU news mailer.

Pulls recent articles from Google News RSS for predefined queries,
renders an HTML digest, and sends it via Gmail SMTP + Telegram.
Run on GitHub Actions every morning at 09:30 KST.
"""

from __future__ import annotations

import html
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path
from urllib.parse import quote_plus

import feedparser

from actions_utils import already_sent_today_kst
from mailer import send_html_email
from news_utils import strip_source_suffix
from notifier import send_telegram_message

KST = timezone(timedelta(hours=9))
LOOKBACK_HOURS = 24

OUTPUT_DIR = Path(__file__).parent / "output"
PUBLIC_REPORT_PATH = OUTPUT_DIR / "public" / "news" / "index.html"

QUERIES: list[tuple[str, str]] = [
    ("AI", '"Claude" OR "Anthropic"'),
    ("AI", '"Gemini" Google'),
    ("AI", '"Grok" xAI'),
    ("AI", '"ChatGPT" OR "OpenAI" OR "GPT-5"'),
    ("GPU", '"NVIDIA" (출시 OR release OR launch OR 발표)'),
    ("GPU", '"RTX" (출시 OR release OR launch)'),
]
CATEGORY_ICONS = {"AI": "🤖", "GPU": "💻"}
# 텔레그램에서 카테고리별 시각 구분용 컬러 불릿 (AI=파랑, GPU=주황)
CATEGORY_BULLETS = {"AI": "🔹", "GPU": "🔸"}


@dataclass
class Article:
    category: str
    title: str
    url: str
    source: str
    published: datetime


def _rss_url(query: str) -> str:
    return (
        f"https://news.google.com/rss/search?q={quote_plus(query)}"
        "&hl=ko&gl=KR&ceid=KR:ko"
    )


def _extract_source(entry) -> str:
    src = entry.get("source")
    if not src:
        return ""
    if hasattr(src, "title") and src.title:
        return src.title
    if isinstance(src, dict):
        return src.get("title", "") or ""
    if isinstance(src, str):
        return src
    return ""


def fetch_articles() -> list[Article]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    seen_titles: set[str] = set()
    out: list[Article] = []

    for category, query in QUERIES:
        feed = feedparser.parse(_rss_url(query))
        for entry in feed.entries:
            link = entry.get("link", "")
            title = entry.get("title", "").strip()
            if not link or not title:
                continue
            dedup_key = title.lower()
            if dedup_key in seen_titles:
                continue

            published_struct = entry.get("published_parsed")
            if not published_struct:
                continue
            published = datetime(*published_struct[:6], tzinfo=timezone.utc)
            if published < cutoff:
                continue

            seen_titles.add(dedup_key)
            src = _extract_source(entry)
            out.append(
                Article(
                    category=category,
                    # Google News RSS 가 제목 끝에 " - <출처>"를 붙여 보내므로,
                    # 출처 컬럼과 중복되지 않게 여기서 한 번 떼어낸다.
                    title=strip_source_suffix(title, src),
                    url=link,
                    source=src,
                    published=published.astimezone(KST),
                )
            )

    out.sort(key=lambda a: (a.category, -a.published.timestamp()))
    return out


def render_email_html(articles: list[Article]) -> str:
    today = datetime.now(KST).strftime("%Y.%m.%d")

    if not articles:
        body = "<p style='color:#666'>지난 24시간 동안 새로운 기사가 없습니다.</p>"
    else:
        sections = []
        for category, group in groupby(articles, key=lambda a: a.category):
            icon = CATEGORY_ICONS.get(category, "📰")
            rows = "".join(
                "<tr>"
                f"<td style='padding:6px 10px;white-space:nowrap;color:#666'>"
                f"{a.published.strftime('%m-%d %H:%M')}</td>"
                f"<td style='padding:6px 10px'>"
                f"<a href=\"{html.escape(a.url, quote=True)}\" "
                f"style='color:#1a73e8;text-decoration:none'>"
                f"{html.escape(a.title)}</a></td>"
                f"<td style='padding:6px 10px;color:#444'>{html.escape(a.source)}</td>"
                "</tr>"
                for a in group
            )
            sections.append(
                f"<h2 style='margin-top:24px;color:#202124;font-size:16px'>"
                f"{icon} {html.escape(category)} 뉴스</h2>"
                "<table style='border-collapse:collapse;width:100%;"
                "border:1px solid #ddd;font-size:14px'>"
                "<thead style='background:#f8f9fa;text-align:left'>"
                "<tr><th style='padding:8px 10px'>발행</th>"
                "<th style='padding:8px 10px'>제목</th>"
                "<th style='padding:8px 10px'>출처</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )
        body = "\n".join(sections)

    return (
        "<!doctype html><html><body style=\"font-family:-apple-system,"
        "BlinkMacSystemFont,'Segoe UI','Malgun Gothic',sans-serif;"
        "max-width:900px;margin:0 auto;padding:16px;color:#202124\">"
        f"<h1 style='border-bottom:2px solid #1a73e8;padding-bottom:8px'>"
        f"🗞 일일 AI/GPU 뉴스 ({today})</h1>"
        f"{body}"
        "<p style='margin-top:24px;color:#999;font-size:12px;text-align:center'>"
        "Google News RSS · 최근 24시간 · 자동 발송</p>"
        "</body></html>"
    )


def render_telegram_message(articles: list[Article], today_str: str) -> str:
    """Telegram HTML 메시지: 카테고리별 최신 기사 5건씩."""
    parts = [f"🗞 <b>일일 AI/GPU 뉴스 ({today_str})</b>", ""]

    by_cat: dict[str, list[Article]] = {}
    for a in articles:
        by_cat.setdefault(a.category, []).append(a)

    for category, items in by_cat.items():
        icon = CATEGORY_ICONS.get(category, "📰")
        bullet = CATEGORY_BULLETS.get(category, "•")
        parts.append(
            f"{icon} <b>{html.escape(category)} 뉴스</b> "
            f"<i>(총 {len(items)}건)</i>"
        )
        for a in items[:5]:
            # 제목은 <a>(Telegram 링크 색상=강조 블루),
            # 출처는 <code>(모노스페이스+회색 배경)으로 색을 분리해 가독성 향상.
            src_html = (
                f"  <code>{html.escape(a.source)}</code>" if a.source else ""
            )
            parts.append(
                f'{bullet} <a href="{html.escape(a.url, quote=True)}">'
                f"{html.escape(a.title)}</a>{src_html}"
            )
        parts.append("")

    return "\n".join(parts).rstrip()


def _should_send_telegram() -> bool:
    if "--telegram" in sys.argv:
        return True
    return os.environ.get("SEND_TELEGRAM", "").strip().lower() in ("1", "true", "yes")


def main() -> int:
    # 멱등성: cron 슬롯이 여러 개라 같은 날 중복 트리거될 수 있다. 이미 오늘
    # 성공한 run 이 있으면 즉시 종료해 메일·텔레그램 중복 발송을 막는다.
    if already_sent_today_kst():
        print("✓ 오늘(KST) 이미 발송 완료 — 종료", file=sys.stderr)
        return 0

    articles = fetch_articles()
    print(f"Fetched {len(articles)} articles", file=sys.stderr)

    today = datetime.now(KST).strftime("%Y.%m.%d")
    html_body = render_email_html(articles)

    # GitHub Pages 호스팅용 사본
    PUBLIC_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_REPORT_PATH.write_text(html_body, encoding="utf-8")
    print(f"🌐 공개본: {PUBLIC_REPORT_PATH}", file=sys.stderr)

    # 이메일
    send_html_email(f"일일 AI/GPU 뉴스 ({today})", html_body)
    print("📧 이메일 발송 완료", file=sys.stderr)

    # 텔레그램 (옵션)
    if _should_send_telegram():
        try:
            send_telegram_message(render_telegram_message(articles, today))
            print("💬 텔레그램 발송 완료", file=sys.stderr)
        except KeyError as e:
            print(f"[오류] 환경변수 누락: {e} "
                  "(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 필요)", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"[오류] 텔레그램 발송 실패: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
