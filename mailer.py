"""Shared Gmail SMTP mailer for daily-digest scripts.

읽는 환경변수:
  GMAIL_USER          (필수) 발송용 Gmail 주소
  GMAIL_APP_PASSWORD  (필수) Gmail 앱 비밀번호 (16자리)
  RECIPIENT_EMAIL     (선택) 콤마/세미콜론/줄바꿈 구분 다중 수신자. 미설정 시 발신자 본인.
  RECIPIENT_BCC       (선택) 동일 형식. SMTP envelope 으로만 전달, 메시지 헤더엔 미포함.

발신자 표시명은 기본 'XEROS'. 필요 시 호출 시점에 sender_name 파라미터로 오버라이드.
"""

from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

DEFAULT_SENDER_NAME = "XEROS"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465


def parse_recipients(raw: str) -> list[str]:
    """콤마/세미콜론/줄바꿈 구분 문자열 → 빈값 제거된 주소 리스트."""
    return [
        addr.strip()
        for addr in raw.replace(";", ",").replace("\n", ",").split(",")
        if addr.strip()
    ]


def send_html_email(subject: str, html_body: str, *,
                    sender_name: str = DEFAULT_SENDER_NAME) -> None:
    """Gmail SMTP 로 HTML 이메일 발송. 다중 수신자 + BCC 지원."""
    user = os.environ["GMAIL_USER"]
    password = os.environ["GMAIL_APP_PASSWORD"]
    to_list = parse_recipients(os.environ.get("RECIPIENT_EMAIL", "")) or [user]
    bcc_list = parse_recipients(os.environ.get("RECIPIENT_BCC", ""))

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = formataddr((sender_name, user))
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.login(user, password)
        smtp.send_message(msg, to_addrs=to_list + bcc_list)
