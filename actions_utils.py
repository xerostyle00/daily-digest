"""GitHub Actions runtime helpers — daily-digest 멱등성 검사용.

여러 cron 슬롯을 등록해 schedule 지연에 대응하는 전략(B 안)에서, 같은 날
중복 발송을 방지하기 위해 GitHub Actions API 로 "오늘(KST) 이 워크플로의
성공 run 이 이미 있는지"를 조회한다.

로컬 실행(워크플로 컨텍스트 밖)에서는 항상 False — fail-open 으로 처리해
개발/수동 테스트가 영향받지 않도록 한다.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
GH_API = "https://api.github.com"


def already_sent_today_kst(workflow_name: str | None = None) -> bool:
    """오늘(KST 날짜 기준) 이 워크플로의 성공한 run 이 이미 존재하면 True.

    GitHub Actions 환경 변수가 없거나 API 호출이 실패하면 False (fail-open) —
    조회 자체 실패로 발송이 막히는 회귀를 만들지 않는다.
    """
    workflow_name = workflow_name or os.environ.get("GITHUB_WORKFLOW", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    current_run_id = os.environ.get("GITHUB_RUN_ID", "")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")

    if not (workflow_name and repo):
        return False  # 로컬 실행

    today_kst = datetime.now(KST).strftime("%Y-%m-%d")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        # 1) 워크플로 ID 조회
        resp = requests.get(
            f"{GH_API}/repos/{repo}/actions/workflows",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        wf = next(
            (w for w in resp.json().get("workflows", [])
             if w.get("name") == workflow_name),
            None,
        )
        if not wf:
            return False

        # 2) 최근 성공 run 들 검사
        resp = requests.get(
            f"{GH_API}/repos/{repo}/actions/workflows/{wf['id']}/runs",
            params={"per_page": 30, "status": "success"},
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        runs = resp.json().get("workflow_runs", [])

        for run in runs:
            if str(run.get("id")) == current_run_id:
                continue  # 자기 자신 제외
            started_str = run.get("run_started_at") or run.get("created_at")
            if not started_str:
                continue
            started = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            if started.astimezone(KST).strftime("%Y-%m-%d") == today_kst:
                return True
    except Exception as e:
        print(f"[idempotency] check failed (fail-open): {e}", file=sys.stderr)
        return False

    return False
