"""Shared helpers for news ingestion (daily-digest)."""

from __future__ import annotations

import re


def strip_source_suffix(title: str, source: str) -> str:
    """Google News RSS 가 제목 끝에 자동으로 붙이는 ``" - <source>"`` 형태를 제거.

    처리 패턴(끝부분):
      - ``" - 출처"``  (ASCII 하이픈)
      - ``" – 출처"``  (en-dash, U+2013)
      - ``" — 출처"``  (em-dash, U+2014)
      - ``" | 출처"``  (파이프)

    출처가 비어있거나 매칭되지 않으면 원본 제목을 그대로 반환.
    매칭 결과가 빈 문자열이면 안전하게 원본을 돌려준다.
    """
    if not title or not source:
        return title
    pattern = r"\s*[\-–—|]\s*" + re.escape(source.strip()) + r"\s*$"
    cleaned = re.sub(pattern, "", title).rstrip()
    return cleaned if cleaned else title
