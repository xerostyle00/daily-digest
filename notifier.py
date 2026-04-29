"""Shared Telegram bot notifier for daily-digest scripts.

읽는 환경변수:
  TELEGRAM_BOT_TOKEN  (필수) BotFather 가 발급한 봇 토큰
  TELEGRAM_CHAT_ID    (필수) 수신 chat_id (개인/그룹/채널)

토큰·chat_id 누락 시 KeyError. 호출자가 graceful skip 처리할 수 있음.
"""

from __future__ import annotations

import os

import requests

TELEGRAM_API = "https://api.telegram.org"


def send_telegram_message(text: str, *,
                         parse_mode: str = "HTML",
                         disable_web_page_preview: bool = True,
                         timeout: int = 15) -> None:
    """Telegram Bot API 로 메시지 발송. HTML 또는 MarkdownV2 지원."""
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    resp = requests.post(
        f"{TELEGRAM_API}/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
