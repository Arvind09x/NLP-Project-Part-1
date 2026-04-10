from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from fitness_reddit_analyzer.build_app_cache import build_snapshot
from fitness_reddit_analyzer.config import APP_CACHE_PATH, DB_PATH


def load_dashboard_snapshot(*, force_refresh: bool = False) -> dict[str, Any]:
    if force_refresh:
        _load_dashboard_snapshot.cache_clear()
    return _load_dashboard_snapshot()


@lru_cache(maxsize=1)
def _load_dashboard_snapshot() -> dict[str, Any]:
    if APP_CACHE_PATH.exists():
        payload = json.loads(APP_CACHE_PATH.read_text(encoding="utf-8"))
        payload.setdefault("meta", {})
        payload["meta"]["cache_source"] = "app_cache"
        return payload
    if DB_PATH.exists():
        payload = build_snapshot()
        payload.setdefault("meta", {})
        payload["meta"]["cache_source"] = "live_sqlite_fallback"
        return payload
    return empty_snapshot()


def empty_snapshot() -> dict[str, Any]:
    return {
        "meta": {
            "cache_source": "empty",
            "selected_window": {"label": "Unavailable"},
        },
        "stats": {
            "total_posts": 0,
            "total_comments": 0,
            "total_authors": 0,
            "modeled_authors": 0,
            "eligible_model_documents": 0,
            "modeled_documents": 0,
            "modeled_posts": 0,
            "modeled_comments": 0,
            "total_topics": 0,
            "major_topic_count": 0,
            "stance_topic_count": 0,
            "window_label": "Unavailable",
            "corpus_summary": "Pipeline outputs are not available yet.",
        },
        "overview": {
            "summary_line": "Pipeline outputs are not available yet.",
            "monthly_activity": [],
            "activity_highlights": {},
            "top_topics": [],
        },
        "topics": [],
        "topic_table": [],
        "stance": {
            "selected_topic_ids": [],
            "skipped_topics": [],
            "analyzed_topics": [],
        },
        "methods": {
            "notes": [],
            "sections": [],
        },
    }
