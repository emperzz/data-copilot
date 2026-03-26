"""Identifier normalization for catalog uniqueness keys."""


def normalize_sql_identifier(value: str | None) -> str:
    """Normalize catalog/db/table names for stable UNIQUE constraints."""
    if value is None:
        return ""
    stripped = value.strip()
    if not stripped:
        return ""
    return stripped.casefold()
