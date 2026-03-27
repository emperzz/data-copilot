"""Shared identifier normalization helpers for SQL-related modules."""


def normalize_sql_identifier(value: str | None) -> str:
    """Normalize catalog/db/table names for stable keys."""
    if value is None:
        return ""
    stripped = value.strip()
    if not stripped:
        return ""
    return stripped.casefold()
