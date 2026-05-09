from __future__ import annotations

from typing import Iterable


def parse_csv_values(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def normalize_set(values: Iterable[str] | None) -> set[str]:
    if values is None:
        return set()
    return {value.strip().lower() for value in values if value and value.strip()}


def category_matches(category: str | None, category_allowlist: set[str]) -> bool:
    if not category_allowlist:
        return True
    if not category:
        return False
    return category.strip().lower() in category_allowlist


def title_matches(title: str | None, title_terms: set[str]) -> bool:
    if not title_terms:
        return True
    if not title:
        return False
    title_lower = title.lower()
    return any(term in title_lower for term in title_terms)


def external_id_matches(external_id: str | None, external_id_allowlist: set[str]) -> bool:
    if not external_id_allowlist:
        return True
    if not external_id:
        return False
    return external_id.strip().lower() in external_id_allowlist
