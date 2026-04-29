"""Shared Gemini-based news title summarizer.

읽는 환경변수:
  GEMINI_API_KEY  (선택) — 없거나 호출 실패 시 summarize_titles 가 None 반환

호출자는 None 처리해 graceful skip 가능 (요약 생략하고 발송 진행).
"""

from __future__ import annotations

import html
import os
import sys

import requests

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)


def summarize_titles(
    titles: list[str],
    *,
    domain: str,
    concrete_examples: str = "구체적 숫자·고유명사·종목명",
) -> list[str] | None:
    """Gemini 로 뉴스 제목 → 한국어 불릿 3줄. 키 없거나 실패 시 None.

    domain: 도메인 컨텍스트 (예: '국제유가', 'AI·GPU 산업')
    concrete_examples: '구체적 ~ 포함' 가이드에 들어갈 도메인별 예시 문구
    """
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key or not titles:
        return None

    titles_text = "\n".join(f"- {t}" for t in titles)
    prompt = (
        f"다음은 오늘 수집된 {domain} 관련 한국어 뉴스 제목 목록입니다.\n"
        "전체를 종합해 가장 중요한 핵심 메시지 3가지를 정보가 충분한 한국어 불릿로 요약하세요.\n\n"
        "규칙:\n"
        "- 정확히 3줄\n"
        "- 각 줄은 한 문장으로 작성하되, 60~120자 분량으로 충분한 내용을 담을 것\n"
        "- 단순 키워드 나열 금지 — 무엇이 일어났고 어떤 영향을 줬는지 인과를 한 문장에 담을 것\n"
        f"- 가능하면 {concrete_examples} 포함\n"
        "- 제목에 명시된 사실만 사용. 추측·과장 금지\n"
        "- 동일 사건이 여러 번 나오면 하나로 묶되 가장 정보량이 많은 형태로 표현\n"
        "- 문체: 각 문장을 명사형 어미 '~임/~함/~됨'으로 종결 "
        "(예: '재돌파임', '강세 보임', '확대됨'). 평서형 '~했다', '~한다' 지양\n"
        '- 출력은 "- " 로 시작하는 불릿만, 다른 설명 텍스트 금지\n\n'
        f"뉴스 제목:\n{titles_text}"
    )

    try:
        resp = requests.post(
            f"{GEMINI_ENDPOINT}?key={api_key}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                # Gemini 2.5 Flash 는 thinking 토큰을 출력 예산에서 먼저 소비하므로
                # 한국어 출력 3줄 + 추론 여유분으로 넉넉히.
                "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2000},
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        candidate = data["candidates"][0]
        text = candidate["content"]["parts"][0]["text"].strip()
        finish_reason = candidate.get("finishReason", "")
        if finish_reason and finish_reason != "STOP":
            print(f"[경고] Gemini finish_reason={finish_reason} (응답 잘림 가능)",
                  file=sys.stderr)
    except Exception as e:
        print(f"[경고] Gemini 요약 실패 (요약 생략): {e}", file=sys.stderr)
        return None

    bullets: list[str] = []
    for line in text.splitlines():
        s = line.strip().lstrip("-•*").strip()
        if s:
            bullets.append(s)
    return bullets[:3] if bullets else None


def render_summary_block_html(bullets: list[str] | None) -> str:
    """이메일 본문 상단의 '오늘의 핵심 내용' 박스. 빈 입력이면 빈 문자열."""
    if not bullets:
        return ""
    items = "".join(
        f"<li style='padding:3px 0;color:#333'>{html.escape(b)}</li>"
        for b in bullets
    )
    return (
        "<div style='background:#fffbf2;border-left:3px solid #ffb74d;"
        "padding:12px 16px;margin:8px 0 4px;border-radius:4px'>"
        "<div style='font-size:13px;color:#888;font-weight:500;margin-bottom:6px'>"
        "📌 오늘의 핵심 내용 <span style='color:#bbb;font-weight:normal'>"
        "(AI 요약)</span></div>"
        "<ul style='margin:0;padding-left:20px;font-size:14px;line-height:1.6'>"
        f"{items}"
        "</ul></div>"
    )
