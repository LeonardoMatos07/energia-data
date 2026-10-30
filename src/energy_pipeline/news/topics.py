"""Deterministic topic classification by keywords (rules in news_topics.yaml, no AI)."""

from __future__ import annotations

import re
import unicodedata
from functools import cache
from pathlib import Path

import yaml

RULES_FILE = Path(__file__).with_name("news_topics.yaml")
FALLBACK_TOPIC = "outros"


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text.lower())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


@cache
def load_rules(path: Path = RULES_FILE) -> dict[str, list[re.Pattern[str]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        topic: [re.compile(rf"\b{_fold(keyword)}") for keyword in keywords]
        for topic, keywords in raw["topics"].items()
    }


def classify(title: str, summary: str | None, path: Path = RULES_FILE) -> list[str]:
    text = _fold(f"{title} {summary or ''}")
    topics = [
        topic
        for topic, patterns in load_rules(path).items()
        if any(pattern.search(text) for pattern in patterns)
    ]
    return topics or [FALLBACK_TOPIC]
