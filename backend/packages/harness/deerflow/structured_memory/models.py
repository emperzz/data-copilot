"""Pydantic models and serialization helpers for structured memory table entities.

The table entity uses a 3-layer structure:
  1. Basic Info — database, tablename, update_frequency
  2. Compiled Truth — objective, definition, core_logic, source_tables, columns, sql
  3. Timeline — append-only audit log of changes
"""

import json
import re
import yaml
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


# ── Business Entity Models ────────────────────────────────────────────────────


class BusinessFieldEntry(BaseModel):
    """A single key-value attribute within a business entity."""
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, description="Attribute name")
    value: str = Field(default="", description="Attribute value")
    description: str = Field(default="", description="Optional description of this attribute")


class BusinessEntity(BaseModel):
    """Business concept / system-level entity (distinct from table entities).

    Used for facts/business/*.md files which describe business definitions,
    tracking systems, data standards, etc.
    """
    model_config = ConfigDict(extra="forbid")

    # ── Identity ─────────────────────────────────────────
    title: str = Field(..., min_length=1, description="Entity title/name")
    business_type: str = Field(default="", description="Business type, e.g. C端电商、内部工具")
    version: str = Field(default="", description="Current version number")

    # ── Description ───────────────────────────────────────
    description: str = Field(default="", description="One-sentence summary")
    background: str = Field(default="", description="Business background/purpose (multi-line)")

    # ── Attributes ────────────────────────────────────────
    attributes: list[BusinessFieldEntry] = Field(
        default_factory=list,
        description="Flat key-value attributes, e.g. 所属平台、数据仓库栏目ID",
    )
    # Keyed by attribute name, each value is a list of string items
    list_attributes: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Multi-value attributes stored as lists, e.g. {核心业务目标: [...], 支持平台: [...]}",
    )

    # ── Relations ─────────────────────────────────────────
    related_entities: list[str] = Field(
        default_factory=list,
        description="Related entity paths, e.g. facts/business/automall_event_naming_convention.md",
    )

    # ── Audit ─────────────────────────────────────────────
    timeline: list[TimelineEntry] = Field(default_factory=list)
    created_at: str | None = Field(None)
    updated_at: str | None = Field(None)


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
            line += f"  \n  - [link]({st.memory_link})"
        lines.append(line)
    return "\n".join(lines)


def _format_columns(columns: list[TableColumn]) -> str:
    """Render columns list as a markdown table."""
    if not columns:
        return f"| {_SENTINEL_NONE} | |"
    lines = ["| column | description |", "|--------|-------------|"]
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


def _serialize_table_entity_to_frontmatter(entity: TableEntity) -> dict:
    """Serialize TableEntity core fields to frontmatter dict."""
    return {
        "tablename": entity.basic_info.tablename,
        "database": entity.basic_info.database,
        "update_frequency": entity.basic_info.update_frequency,
        "objective": entity.compiled_truth.objective,
        "definition": entity.compiled_truth.definition,
        "core_logic": entity.compiled_truth.core_logic,
        "created_at": entity.created_at,
        "updated_at": entity.updated_at,
    }


def _parse_frontmatter(content: str) -> tuple[dict | None, str]:
    """Parse YAML frontmatter from markdown content.

    Returns (frontmatter_dict, body_without_frontmatter) or (None, original_content).
    """
    if not content.strip().startswith("---"):
        return None, content

    parts = content.split("---", 2)
    if len(parts) < 3:
        return None, content

    fm_raw, body = parts[1], parts[2].strip()
    try:
        fm = yaml.safe_load(fm_raw)
        if not isinstance(fm, dict):
            return None, content
        return fm, body
    except yaml.YAMLError:
        return None, content


def _parse_from_frontmatter(fm: dict, body: str) -> TableEntity:
    """Parse TableEntity from frontmatter dict and markdown body."""
    # Basic Info
    tablename = str(fm.get("tablename", "")) or "(unknown)"
    database = str(fm.get("database", ""))
    update_freq = fm.get("update_frequency", "daily")
    if update_freq not in {"daily", "hourly", "weekly", "monthly", "yearly", "realtime", "onetime"}:
        update_freq = "daily"

    bi = TableBasicInfo(
        tablename=tablename,
        database=database,
        update_frequency=update_freq,  # type: ignore[arg-type]
    )

    # Compiled Truth fields from frontmatter
    objective = str(fm.get("objective", ""))
    definition = str(fm.get("definition", ""))
    core_logic = str(fm.get("core_logic", ""))

    # Parse source_tables, columns, sql from body
    source_tables = _parse_source_tables(body)
    columns = _parse_columns(body)

    sql: str | None = None
    sql_m = re.search(r"```sql\s*\r?\n(.*?)\n```", body, re.DOTALL)
    if sql_m:
        sql_block = sql_m.group(1).strip()
        sql = sql_block if sql_block else None

    ct = TableCompiledTruth(
        objective=objective,
        definition=definition,
        core_logic=core_logic,
        source_tables=source_tables,
        columns=columns,
        sql=sql,
    )

    # Timeline from body
    timeline = _parse_timeline(body)

    # Timestamps from frontmatter
    created_at = str(fm.get("created_at", "")) or None
    updated_at = str(fm.get("updated_at", "")) or None

    return TableEntity(
        basic_info=bi,
        compiled_truth=ct,
        timeline=timeline,
        created_at=created_at,
        updated_at=updated_at,
    )


def table_entity_to_markdown(entity: TableEntity) -> str:
    """Serialize a TableEntity to the structured 3-layer markdown format with YAML frontmatter."""
    # 1. Generate YAML frontmatter
    fm = _serialize_table_entity_to_frontmatter(entity)
    fm = {k: v for k, v in fm.items() if v is not None}
    fm_yaml = yaml.dump(fm, allow_unicode=True, sort_keys=False, default_flow_style=False).rstrip()

    # 2. Generate markdown body
    bi = entity.basic_info
    ct = entity.compiled_truth

    source_tables_md = _format_source_tables(ct.source_tables)
    columns_md = _format_columns(ct.columns)
    timeline_md = _format_timeline(entity.timeline)
    sql_block = ct.sql if ct.sql else ""

    sql_section = f"```sql\n{sql_block}\n```" if sql_block else "```sql\n\n```"

    created = entity.created_at or ""
    updated = entity.updated_at or ""

    md_body = f"""# {bi.tablename}

## Basic Info

- **database**: {bi.database}
- **table**: {bi.tablename}
- **update_frequency**: {bi.update_frequency}

## Compiled Truth

- **objective**: {ct.objective}
- **definition**: {ct.definition}
- **core_logic**: {ct.core_logic}

### upstream dependencies

{source_tables_md}

### columns

{columns_md}

### SQL
{sql_section}

## Timeline

{timeline_md}

---
*created: {created}*
*updated: {updated}*
"""

    return f"---\n{fm_yaml}\n---\n\n{md_body}"


# ── Deserialization Helpers (Module Level) ──────────────────────────────────


def _is_sentinel(value: str) -> bool:
    return value.strip() == _SENTINEL_NONE


def _parse_source_tables(text: str) -> list[SourceTable]:
    """Parse upstream dependencies section into SourceTable list."""
    m = re.search(r"### upstream dependencies\s*\r?\n+(.+?)(?=\n###|\n##|\n---|\Z)", text, re.DOTALL)
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
        link_m = re.search(r"\[link\]\((.+?)\)", entry)
        results.append(SourceTable(full_name=full_name, memory_link=link_m.group(1) if link_m else None))
    return results


def _parse_columns(text: str) -> list[TableColumn]:
    """Parse columns markdown table into TableColumn list."""
    m = re.search(r"### columns\s*\r?\n+(.+?)(?=\n###|\n##|\n---|\Z)", text, re.DOTALL)
    if not m:
        return []
    section = m.group(1)
    results: list[TableColumn] = []
    for line in section.strip().split("\n"):
        line = line.strip()
        if not line.startswith("|") or line.startswith("|---"):
            continue
        # Skip header row
        if "column" in line and "description" in line:
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


# ── Deserialization Helper ───────────────────────────────────────────────────


def markdown_to_table_entity(content: str) -> TableEntity:
    """Best-effort parse of markdown content back into a TableEntity.

    First tries YAML frontmatter, then falls back to regex parsing.
    """
    if not content or not content.strip():
        return TableEntity(
            basic_info=TableBasicInfo(tablename="(unknown)"),
            compiled_truth=TableCompiledTruth(),
        )

    # 1. Try frontmatter parsing first
    fm, body = _parse_frontmatter(content)
    if fm:
        return _parse_from_frontmatter(fm, body)

    # 2. Fall back to regex parsing (backward compatibility)
    def _extract(pattern: str, text: str, default: str = "") -> str:
        m = re.search(pattern, text)
        return m.group(1).strip() if m else default

    def _is_field_label(line: str) -> bool:
        """Check if a line is a field label (e.g., '- **objective**: value')."""
        return bool(re.match(r"^\s*-\s+\*\*.+?\*\*:", line.strip()))

    def _extract_field(text: str, field_name: str) -> str:
        """Extract field value by parsing line by line.

        Handles both single-line values and multi-line values that continue
        until the next section header or field label.
        """
        lines = text.split("\n")
        for i, line in enumerate(lines):
            if f"**{field_name}**" not in line:
                continue
            # Value starts after the ': '
            after_label = line.split(f"**{field_name}**:", 1)[1]
            value_parts = [after_label.strip()] if after_label.strip() else []

            # Collect continuation lines until section header or next field label
            j = i + 1
            while j < len(lines):
                l = lines[j].strip()
                if not l:
                    j += 1
                    continue
                if l.startswith("### "):
                    break
                if _is_field_label(l):
                    break
                value_parts.append(l)
                j += 1

            return " ".join(value_parts)
        return ""

    def _extract_frequency(text: str) -> str:
        raw = _extract_field(text, "update_frequency")
        valid = {"daily", "hourly", "weekly", "monthly", "yearly", "realtime", "onetime"}
        return raw if raw in valid else "daily"

    database = _extract_field(content, "database")
    tablename = _extract_field(content, "table")
    update_frequency = _extract_frequency(content)
    objective = _extract_field(content, "objective")
    definition = _extract_field(content, "definition")
    core_logic = _extract_field(content, "core_logic")

    # Extract SQL block
    sql: str | None = None
    sql_m = re.search(r"```sql\s*\r?\n(.*?)\n```", content, re.DOTALL)
    if sql_m:
        sql_block = sql_m.group(1).strip()
        sql = sql_block if sql_block else None

    created_at = _extract(r"\*created:\s*(.+?)(?:\n|$|\*)", content)
    updated_at = _extract(r"\*updated:\s*(.+?)(?:\n|$|\*)", content)

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
    in Basic Info and Compiled Truth are preserved as-is. A new timeline entry
    is appended with the current timestamp and ``timeline_desc``.

    Implementation: parse → mutate Pydantic model → re-serialize.
    This avoids regex-based section replacement which loses multi-entry sections.

    Args:
        existing_markdown: The current full markdown content of the entity.
        changes: Dict mapping field names to new values. Supported keys:
            database, tablename, update_frequency (basic info),
            objective, definition, core_logic, source_tables, columns,
            column_updates, sql. The columns key replaces all columns;
            column_updates merges by matching column names.
        timeline_desc: Description for the new timeline entry (e.g. "updated definition").

    Returns:
        The complete updated markdown with changes applied and timeline appended.
    """
    if not changes and not timeline_desc:
        return existing_markdown

    # ── 1. Parse existing markdown into model ──────────────────────────────────
    entity = markdown_to_table_entity(existing_markdown)
    now = _now_iso()

    # ── 2. Apply simple field changes ─────────────────────────────────────────
    bi = entity.basic_info
    ct = entity.compiled_truth

    if "database" in changes:
        bi.database = str(changes["database"])
    if "table" in changes:
        bi.tablename = str(changes["table"])
    elif "tablename" in changes:
        bi.tablename = str(changes["tablename"])
    if "update_frequency" in changes:
        bi.update_frequency = changes["update_frequency"]  # type: ignore[arg-type]
    if "objective" in changes:
        ct.objective = str(changes["objective"])
    if "definition" in changes:
        ct.definition = str(changes["definition"])
    if "core_logic" in changes:
        ct.core_logic = str(changes["core_logic"])

    # ── 3. Apply complex list fields ─────────────────────────────────────────
    if "source_tables" in changes:
        st_list = changes["source_tables"]
        if isinstance(st_list, list):
            ct.source_tables = []
            for item in st_list:
                if isinstance(item, dict):
                    ct.source_tables.append(SourceTable(**item))
                elif isinstance(item, str):
                    ct.source_tables.append(SourceTable(full_name=item))

    if "columns" in changes:
        col_list = changes["columns"]
        if isinstance(col_list, list):
            ct.columns = []
            for item in col_list:
                if isinstance(item, dict):
                    ct.columns.append(TableColumn(**item))
                elif isinstance(item, str):
                    ct.columns.append(TableColumn(name=item, description=""))

    if "column_updates" in changes:
        col_updates = changes["column_updates"]
        if isinstance(col_updates, list):
            updates_map = {item["name"]: item for item in col_updates if isinstance(item, dict) and "name" in item}
            for col in ct.columns:
                if col.name in updates_map:
                    new_desc = updates_map[col.name].get("description")
                    if new_desc is not None:
                        col.description = new_desc

    if "sql" in changes:
        sql_val = changes["sql"]
        ct.sql = str(sql_val) if sql_val else None

    # ── 4. Append timeline entry ───────────────────────────────────────────────
    if timeline_desc:
        entity.timeline.append(TimelineEntry(time=now, content=timeline_desc))

    # ── 5. Update timestamps ─────────────────────────────────────────────────
    entity.updated_at = now
    if not entity.created_at:
        entity.created_at = now

    # ── 6. Re-serialize ─────────────────────────────────────────────────────
    return table_entity_to_markdown(entity)


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


# ── Business Entity Serialization / Deserialization ────────────────────────


def _format_business_attributes(attributes: list[BusinessFieldEntry]) -> str:
    """Render attributes as a markdown table."""
    if not attributes:
        return f"| {_SENTINEL_NONE} | | |"
    lines = ["| attribute | value | description |", "|-----------|-------|-------------|"]
    for attr in attributes:
        desc_escaped = attr.description.replace("\n", " ").replace("|", "\\|")
        val_escaped = attr.value.replace("\n", " ").replace("|", "\\|")
        lines.append(f"| {attr.name} | {val_escaped} | {desc_escaped} |")
    return "\n".join(lines)


def _format_business_list_attributes(list_attrs: dict[str, list[str]]) -> str:
    """Render list_attributes as named bullet-point sections."""
    if not list_attrs:
        return _SENTINEL_NONE
    sections: list[str] = []
    for name, items in list_attrs.items():
        lines = [f"### {name}\n"]
        if items:
            for item in items:
                lines.append(f"- {item}")
        else:
            lines.append(f"- {_SENTINEL_NONE}")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def _format_business_related(related: list[str]) -> str:
    """Render related_entities as markdown links."""
    if not related:
        return _SENTINEL_NONE
    lines: list[str] = []
    for entity in related:
        fname = entity.split("/")[-1]
        lines.append(f"- [{fname}]({entity})")
    return "\n".join(lines)


def business_entity_to_markdown(entity: BusinessEntity) -> str:
    """Serialize a BusinessEntity to structured markdown with YAML frontmatter."""
    # 1. Generate YAML frontmatter
    fm = {
        "title": entity.title,
        "business_type": entity.business_type,
        "version": entity.version,
        "description": entity.description,
        "created_at": entity.created_at,
        "updated_at": entity.updated_at,
    }
    fm = {k: v for k, v in fm.items() if v is not None and v != ""}
    fm_yaml = yaml.dump(fm, allow_unicode=True, sort_keys=False, default_flow_style=False).rstrip()

    # 2. Generate markdown body
    attrs_md = _format_business_attributes(entity.attributes)
    list_attrs_md = _format_business_list_attributes(entity.list_attributes)
    related_md = _format_business_related(entity.related_entities)
    timeline_md = _format_timeline(entity.timeline)

    created = entity.created_at or ""
    updated = entity.updated_at or ""

    body = f"""# {entity.title}

## 基本信息

{entity.description}

## 业务背景

{entity.background}

## 业务属性

{attrs_md}

## 核心要点

{list_attrs_md}

## 相关实体

{related_md}

## Timeline

{timeline_md}

---
*created: {created}*
*updated: {updated}*
"""
    return f"---\n{fm_yaml}\n---\n\n{body}"


def _parse_business_attributes(text: str) -> list[BusinessFieldEntry]:
    """Parse the 业务属性 section into BusinessFieldEntry list."""
    m = re.search(r"## 业务属性\s*\r?\n+(.+?)(?=\n##|\n---|\Z)", text, re.DOTALL)
    if not m:
        return []
    section = m.group(1).strip()
    if _is_sentinel(section):
        return []
    results: list[BusinessFieldEntry] = []
    for line in section.strip().split("\n"):
        line = line.strip()
        if not line.startswith("|") or line.startswith("|---"):
            continue
        if "attribute" in line and "value" in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 4 and parts[1] and not _is_sentinel(parts[1]):
            name = parts[1]
            value = parts[2] if len(parts) > 2 else ""
            desc = parts[3] if len(parts) > 3 else ""
            results.append(BusinessFieldEntry(name=name, value=value, description=desc))
    return results


def _parse_business_list_attributes(text: str) -> dict[str, list[str]]:
    """Parse 核心要点 section into dict of string lists."""
    m = re.search(r"## 核心要点\s*\r?\n+(.+?)(?=\n## |\n---|\Z)", text, re.DOTALL)
    if not m:
        return {}
    section = m.group(1).strip()
    if _is_sentinel(section):
        return {}
    result: dict[str, list[str]] = {}
    current_section = ""
    current_items: list[str] = []

    # Split by ### headers
    parts = re.split(r"(?=### )", section)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith("### "):
            if current_section:
                result[current_section] = current_items
            header_match = re.match(r"### (.+)", part)
            if header_match:
                current_section = header_match.group(1).strip()
                current_items = []
                rest = part[len(f"### {current_section}"):].strip()
                if rest:
                    item_lines = [l.strip() for l in rest.split("\n") if l.strip().startswith("- ")]
                    current_items.extend([l[2:].strip() for l in item_lines])
        else:
            item_lines = [l.strip() for l in part.split("\n") if l.strip().startswith("- ")]
            current_items.extend([l[2:].strip() for l in item_lines])

    if current_section:
        result[current_section] = current_items
    return result


def _parse_business_related(text: str) -> list[str]:
    """Parse related entities section into path strings."""
    m = re.search(r"## 相关实体\s*\r?\n+(.+?)(?=\n##|\n---|\Z)", text, re.DOTALL)
    if not m:
        return []
    section = m.group(1).strip()
    if _is_sentinel(section):
        return []
    results: list[str] = []
    for line in section.strip().split("\n"):
        line = line.strip()
        if not line.startswith("- ["):
            continue
        link_m = re.search(r"\]\((.+?)\)", line)
        if link_m:
            results.append(link_m.group(1))
    return results


def markdown_to_business_entity(content: str) -> BusinessEntity:
    """Best-effort parse of markdown content into a BusinessEntity.

    First tries YAML frontmatter, then falls back to regex extraction.
    """
    if not content or not content.strip():
        return BusinessEntity(title="(unknown)")

    fm, body = _parse_frontmatter(content)
    if fm:
        title = str(fm.get("title", "")) or "(unknown)"
        business_type = str(fm.get("business_type", ""))
        version = str(fm.get("version", ""))
        description = str(fm.get("description", ""))
        created_at = str(fm.get("created_at", "")) or None
        updated_at = str(fm.get("updated_at", "")) or None

        entity = BusinessEntity(
            title=title,
            business_type=business_type,
            version=version,
            description=description,
            attributes=_parse_business_attributes(body),
            list_attributes=_parse_business_list_attributes(body),
            related_entities=_parse_business_related(body),
            timeline=_parse_timeline(body),
            created_at=created_at,
            updated_at=updated_at,
        )
        # background from body via regex
        bg_match = re.search(r"## 业务背景\s*\r?\n+(.+?)(?=\n##|\n---|\Z)", body, re.DOTALL)
        if bg_match:
            entity.background = bg_match.group(1).strip()
        return entity

    # Fallback: regex extraction
    title_match = re.search(r"^#\s+(.+)\s*$", content, re.M)
    title = title_match.group(1).strip() if title_match else "(unknown)"

    desc_section = re.search(r"## 基本信息\s*\r?\n+(.+?)(?=\n##|\n---|\Z)", content, re.DOTALL)
    description = desc_section.group(1).strip() if desc_section else ""

    bg_match = re.search(r"## 业务背景\s*\r?\n+(.+?)(?=\n##|\n---|\Z)", content, re.DOTALL)
    background = bg_match.group(1).strip() if bg_match else ""

    return BusinessEntity(
        title=title,
        description=description,
        background=background,
        attributes=_parse_business_attributes(content),
        list_attributes=_parse_business_list_attributes(content),
        related_entities=_parse_business_related(content),
        timeline=_parse_timeline(content),
    )


def parse_business_changes_json(changes_str: str) -> dict:
    """Parse the JSON string for business entity changes.

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


def apply_partial_update_for_business(
    existing_markdown: str,
    changes: dict,
    timeline_desc: str,
) -> str:
    """Apply a partial update to a business entity markdown document.

    Only the fields specified in ``changes`` are modified. All other fields
    are preserved as-is. A new timeline entry is appended.

    Args:
        existing_markdown: The current full markdown content.
        changes: Dict mapping field names to new values. Supported keys:
            title, business_type, version, description, background,
            attributes (list of {name, value, description}),
            list_attributes (dict of {section_name: [items]}),
            related_entities (list of paths).
        timeline_desc: Description for the new timeline entry.

    Returns:
        The complete updated markdown with changes applied and timeline appended.
    """
    if not changes and not timeline_desc:
        return existing_markdown

    entity = markdown_to_business_entity(existing_markdown)
    now = _now_iso()

    # ── Simple fields ───────────────────────────────────────
    if "title" in changes:
        entity.title = str(changes["title"])
    if "business_type" in changes:
        entity.business_type = str(changes["business_type"])
    if "version" in changes:
        entity.version = str(changes["version"])
    if "description" in changes:
        entity.description = str(changes["description"])
    if "background" in changes:
        entity.background = str(changes["background"])

    # ── Attributes (replace all) ────────────────────────────
    if "attributes" in changes:
        attr_list = changes["attributes"]
        if isinstance(attr_list, list):
            entity.attributes = []
            for item in attr_list:
                if isinstance(item, dict):
                    entity.attributes.append(BusinessFieldEntry(**item))
                elif isinstance(item, str):
                    entity.attributes.append(BusinessFieldEntry(name=item, value=""))

    # ── List attributes (merge by section name) ─────────────
    if "list_attributes" in changes:
        la = changes["list_attributes"]
        if isinstance(la, dict):
            for section_name, items in la.items():
                if isinstance(items, list):
                    entity.list_attributes[section_name] = items

    # ── Related entities (replace all) ──────────────────────
    if "related_entities" in changes:
        re_list = changes["related_entities"]
        if isinstance(re_list, list):
            entity.related_entities = [str(r) for r in re_list]

    # ── Timeline ────────────────────────────────────────────
    if timeline_desc:
        entity.timeline.append(TimelineEntry(time=now, content=timeline_desc))

    # ── Timestamps ───────────────────────────────────────────
    entity.updated_at = now
    if not entity.created_at:
        entity.created_at = now

    # ── Re-serialize ────────────────────────────────────────
    return business_entity_to_markdown(entity)
