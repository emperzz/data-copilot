"""Pydantic models and serialization helpers for structured memory table entities.

The table entity uses a 3-layer structure:
  1. 基本信息 (Basic Info) — database, tablename, update_frequency
  2. Compiled Truth — objective, definition, core_logic, source_tables, columns, sql
  3. Timeline — append-only audit log of changes
"""

import json
import re
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ── Pydantic Models ──────────────────────────────────────────────────────────

class SourceTable(BaseModel):
    """An upstream table dependency."""
    model_config = ConfigDict(extra="forbid")

    full_name: str = Field(..., min_length=1, description="Fully qualified name in 'db.table' format")
    memory_link: str | None = Field(None, description="Link to memory entity file if it exists")


class TableColumn(BaseModel):
    """A column definition within a table entity."""
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, description="Column name")
    description: str = Field(default="", description="Detailed column description")


class TimelineEntry(BaseModel):
    """A single entry in the append-only timeline audit log."""
    model_config = ConfigDict(extra="forbid")

    time: str = Field(..., min_length=1, description="ISO-format timestamp, e.g. '2026-04-30 14:30:00'")
    content: str = Field(..., min_length=1, description="Description of what changed; '首次写入' for initial creation")


class TableBasicInfo(BaseModel):
    """Identifying attributes for a database table."""
    model_config = ConfigDict(extra="forbid")

    database: str = Field(default="", description="Data warehouse / database name")
    tablename: str = Field(..., min_length=1, description="Table name — REQUIRED, must never be empty")
    update_frequency: Literal["daily", "hourly", "weekly", "monthly", "yearly", "realtime", "onetime"] = Field(
        default="daily", description="How often the table is refreshed"
    )


class TableCompiledTruth(BaseModel):
    """Curated, definitive information about a table."""
    model_config = ConfigDict(extra="forbid")

    objective: str = Field(default="", description="Purpose of the table; e.g. 原始业务表/维度表/聚合宽表")
    definition: str = Field(default="", description="Where data comes from, how it's calculated, key nodes")
    core_logic: str = Field(default="", description="Core business logic distilled from SQL")
    source_tables: list[SourceTable] = Field(default_factory=list, description="Upstream tables this table depends on")
    columns: list[TableColumn] = Field(default_factory=list, description="Column definitions")
    sql: str | None = Field(None, description="Original SQL; None or empty for source/business tables")


class TableEntity(BaseModel):
    """Top-level table entity combining basic info, compiled truth, and timeline."""
    model_config = ConfigDict(extra="forbid")

    basic_info: TableBasicInfo
    compiled_truth: TableCompiledTruth
    timeline: list[TimelineEntry] = Field(default_factory=list, description="Append-only change audit log")
    created_at: str | None = Field(None, description="ISO timestamp of initial creation")
    updated_at: str | None = Field(None, description="ISO timestamp of last update")


# ── Serialization Helpers ────────────────────────────────────────────────────

_SENTINEL_NONE = "(none)"


def _format_source_tables(source_tables: list[SourceTable]) -> str:
    """Render source_tables list as markdown bullet points."""
    if not source_tables:
        return _SENTINEL_NONE
    lines: list[str] = []
    for st in source_tables:
        line = f"- {st.full_name}"
        if st.memory_link:
            line += f"  \n  - [链接]({st.memory_link})"
        lines.append(line)
    return "\n".join(lines)


def _format_columns(columns: list[TableColumn]) -> str:
    """Render columns list as a markdown table."""
    if not columns:
        return f"| {_SENTINEL_NONE} | |"
    lines = ["| 字段 | 说明 |", "|------|------|"]
    for col in columns:
        desc_escaped = col.description.replace("\n", " ").replace("|", "\\|")
        lines.append(f"| {col.name} | {desc_escaped} |")
    return "\n".join(lines)


def _format_timeline(timeline: list[TimelineEntry]) -> str:
    """Render timeline entries as markdown bullet points."""
    if not timeline:
        return _SENTINEL_NONE
    lines: list[str] = []
    for entry in timeline:
        lines.append(f"- **{entry.time}** — {entry.content}")
    return "\n".join(lines)


def table_entity_to_markdown(entity: TableEntity) -> str:
    """Serialize a TableEntity to the structured 3-layer markdown format."""
    bi = entity.basic_info
    ct = entity.compiled_truth

    source_tables_md = _format_source_tables(ct.source_tables)
    columns_md = _format_columns(ct.columns)
    timeline_md = _format_timeline(entity.timeline)
    sql_block = ct.sql if ct.sql else ""

    sql_section = f"```sql\n{sql_block}\n```" if sql_block else "```sql\n\n```"

    created = entity.created_at or ""
    updated = entity.updated_at or ""

    return f"""# 基本信息

- **库**: {bi.database}
- **表**: {bi.tablename}
- **更新频率**: {bi.update_frequency}

## Compiled Truth

- **目的**: {ct.objective}
- **定义**: {ct.definition}
- **核心逻辑**: {ct.core_logic}

### 上游依赖

{source_tables_md}

### 字段

{columns_md}

### SQL
{sql_section}

## Timeline

{timeline_md}

---
*创建: {created}*
*更新: {updated}*
"""


# ── Deserialization Helper ───────────────────────────────────────────────────


def markdown_to_table_entity(content: str) -> TableEntity:
    """Best-effort parse of markdown content back into a TableEntity.

    Used for reading existing entities from disk. This is inherently lossy
    (markdown is unstructured), so the canonical serialization path is
    ``table_entity_to_markdown``.
    """
    if not content or not content.strip():
        return TableEntity(
            basic_info=TableBasicInfo(tablename="(unknown)"),
            compiled_truth=TableCompiledTruth(),
        )

    def _extract(pattern: str, text: str, default: str = "") -> str:
        m = re.search(pattern, text)
        return m.group(1).strip() if m else default

    def _extract_frequency(text: str) -> str:
        raw = _extract(r"\*\*更新频率\*\*\s*:\s*(.+?)(?:\n|$)", text)
        valid = {"daily", "hourly", "weekly", "monthly", "yearly", "realtime", "onetime"}
        return raw if raw in valid else "daily"

    def _is_sentinel(value: str) -> bool:
        return value.strip() == _SENTINEL_NONE

    def _parse_source_tables(text: str) -> list[SourceTable]:
        """Parse 上游依赖 section into SourceTable list."""
        m = re.search(r"### 上游依赖\s*\r?\n+(.+?)(?=\n###|\n##|\n---|\Z)", text, re.DOTALL)
        if not m:
            return []
        section = m.group(1).strip()
        if _is_sentinel(section):
            return []
        results: list[SourceTable] = []
        entries = re.split(r"\n(?=- )", section)
        for entry in entries:
            entry = entry.strip()
            if not entry.startswith("- "):
                continue
            name_m = re.match(r"- ([^\s]+)", entry)
            if not name_m:
                continue
            full_name = name_m.group(1)
            link_m = re.search(r"\[链接\]\((.+?)\)", entry)
            results.append(SourceTable(full_name=full_name, memory_link=link_m.group(1) if link_m else None))
        return results

    def _parse_columns(text: str) -> list[TableColumn]:
        """Parse 字段 markdown table into TableColumn list."""
        m = re.search(r"### 字段\s*\r?\n+(.+?)(?=\n###|\n##|\n---|\Z)", text, re.DOTALL)
        if not m:
            return []
        section = m.group(1)
        results: list[TableColumn] = []
        for line in section.strip().split("\n"):
            line = line.strip()
            if not line.startswith("|") or line.startswith("|---"):
                continue
            # Skip header row
            if "字段" in line and "说明" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # Expected: ['', 'col_name', 'description', '']
            if len(parts) >= 4 and parts[1] and not _is_sentinel(parts[1]):
                desc = parts[2] if len(parts) > 2 and not _is_sentinel(parts[2]) else ""
                results.append(TableColumn(name=parts[1], description=desc))
        return results

    def _parse_timeline(text: str) -> list[TimelineEntry]:
        """Parse Timeline section into TimelineEntry list."""
        m = re.search(r"## Timeline\s*\r?\n+(.+?)(?=\n---|\Z)", text, re.DOTALL)
        if not m:
            return []
        section = m.group(1).strip()
        if _is_sentinel(section):
            return []
        results: list[TimelineEntry] = []
        for line in section.split("\n"):
            line = line.strip()
            tm = re.match(r"- \*\*(.+?)\*\* — (.+)", line)
            if tm:
                results.append(TimelineEntry(time=tm.group(1).strip(), content=tm.group(2).strip()))
        return results

    database = _extract(r"\*\*库\*\*\s*:\s*(.+?)(?:\n|$)", content)
    tablename = _extract(r"\*\*表\*\*\s*:\s*(.+?)(?:\n|$)", content)
    update_frequency = _extract_frequency(content)
    objective = _extract(r"\*\*目的\*\*\s*:\s*(.+?)(?:\n|$)", content)
    definition = _extract(r"\*\*定义\*\*\s*:\s*(.+?)(?:\n|$)", content)
    core_logic = _extract(r"\*\*核心逻辑\*\*\s*:\s*(.+?)(?:\n|$)", content)

    # Extract SQL block
    sql: str | None = None
    sql_m = re.search(r"```sql\s*\r?\n(.*?)\n```", content, re.DOTALL)
    if sql_m:
        sql_block = sql_m.group(1).strip()
        sql = sql_block if sql_block else None

    created_at = _extract(r"\*创建:\s*(.+?)(?:\n|$|\*)", content)
    updated_at = _extract(r"\*更新:\s*(.+?)(?:\n|$|\*)", content)

    return TableEntity(
        basic_info=TableBasicInfo(
            database=database,
            tablename=tablename,
            update_frequency=update_frequency,  # type: ignore[arg-type]
        ),
        compiled_truth=TableCompiledTruth(
            objective=objective,
            definition=definition,
            core_logic=core_logic,
            source_tables=_parse_source_tables(content),
            columns=_parse_columns(content),
            sql=sql,
        ),
        timeline=_parse_timeline(content),
        created_at=created_at or None,
        updated_at=updated_at or None,
    )


# ── Partial Update Helper ────────────────────────────────────────────────────


# Mapping from model field name to markdown bold label pattern
_FIELD_LABEL_MAP: dict[str, str] = {
    "database": "**库**",
    "tablename": "**表**",
    "update_frequency": "**更新频率**",
    "objective": "**目的**",
    "definition": "**定义**",
    "core_logic": "**核心逻辑**",
}


def _now_iso() -> str:
    """Return current UTC time as ISO-ish string like '2026-04-30 14:30:00'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def apply_partial_update(
    existing_markdown: str,
    changes: dict,
    timeline_desc: str,
) -> str:
    """Apply a partial update to an existing table entity markdown document.

    Only the fields specified in ``changes`` are modified. All other fields
    in 基本信息 and Compiled Truth are preserved as-is. A new timeline entry
    is appended with the current timestamp and ``timeline_desc``.

    Args:
        existing_markdown: The current full markdown content of the entity.
        changes: Dict mapping field names to new values. Supported keys:
            database, tablename, update_frequency (basic info),
            objective, definition, core_logic, source_tables, columns, sql.
        timeline_desc: Description for the new timeline entry (e.g. "更新了定义").

    Returns:
        The complete updated markdown with changes applied and timeline appended.
    """
    result = existing_markdown
    now = _now_iso()

    if not changes:
        return result

    # ── 1. Apply simple field changes ────────────────────────────────────────
    for field_name, label in _FIELD_LABEL_MAP.items():
        if field_name not in changes:
            continue

        new_value = str(changes[field_name])
        pattern = rf"(\*\*{label[2:-2]}\*\*\s*:\s*)(.+?)(\r?\n|$)"

        def _replace_simple(m: re.Match, val: str = new_value) -> str:
            return f"{m.group(1)}{val}{m.group(3)}"

        result = re.sub(pattern, _replace_simple, result, count=1)

    # ── 2. Apply source_tables change ────────────────────────────────────────
    if "source_tables" in changes:
        st_list = changes["source_tables"]
        if isinstance(st_list, list):
            source_tables = []
            for item in st_list:
                if isinstance(item, dict):
                    source_tables.append(SourceTable(**item))
                elif isinstance(item, str):
                    source_tables.append(SourceTable(full_name=item))
            new_section = _format_source_tables(source_tables)
        else:
            new_section = str(st_list)
        result = re.sub(
            r"(### 上游依赖\s*\r?\n+).+?(?=\n###|\n##|\n---|\Z)",
            rf"\g<1>{new_section}",
            result,
            count=1,
            flags=re.DOTALL,
        )

    # ── 3. Apply columns change ──────────────────────────────────────────────
    if "columns" in changes:
        col_list = changes["columns"]
        if isinstance(col_list, list):
            columns = []
            for item in col_list:
                if isinstance(item, dict):
                    columns.append(TableColumn(**item))
                elif isinstance(item, str):
                    columns.append(TableColumn(name=item, description=""))
            new_section = _format_columns(columns)
        else:
            new_section = str(col_list)
        result = re.sub(
            r"(### 字段\s*\r?\n+).+?(?=\n###|\n##|\n---|\Z)",
            rf"\g<1>{new_section}",
            result,
            count=1,
            flags=re.DOTALL,
        )

    # ── 4. Apply SQL change ──────────────────────────────────────────────────
    if "sql" in changes:
        new_sql = str(changes["sql"]) if changes["sql"] else ""
        sql_block = f"```sql\n{new_sql}\n```"
        result = re.sub(
            r"```sql\s*\r?\n.*?\n```",
            sql_block,
            result,
            count=1,
            flags=re.DOTALL,
        )

    # ── 5. Append new timeline entry ─────────────────────────────────────────
    if timeline_desc:
        new_entry = f"- **{now}** — {timeline_desc}"
        if "## Timeline" in result:
            result = re.sub(
                r"(## Timeline\s*\r?\n)",
                rf"\g<1>{new_entry}\n",
                result,
                count=1,
            )
        else:
            result = re.sub(
                r"(\n---\n)",
                f"\n## Timeline\n\n{new_entry}\n\\1",
                result,
                count=1,
            )

    # ── 6. Update the '更新' timestamp ───────────────────────────────────────
    if "*更新:" in result:
        result = re.sub(r"\*更新:\s*.*?\*", f"*更新: {now}*", result, count=1)
    else:
        result = re.sub(r"(\*创建:.*?\*)", rf"\g<1>\n*更新: {now}*", result, count=1)

    return result


def parse_changes_json(changes_str: str) -> dict:
    """Parse the JSON string from the tool's ``changes`` parameter into a dict.

    Returns an empty dict if the string is empty or invalid.
    """
    if not changes_str or not changes_str.strip():
        return {}
    try:
        parsed = json.loads(changes_str)
        if not isinstance(parsed, dict):
            return {}
        return parsed
    except (json.JSONDecodeError, TypeError):
        return {}
