"""Explicit structured memory write tool (Chroma tiers)."""

from __future__ import annotations

from typing import Literal

from langchain.tools import ToolRuntime, tool
from langgraph.config import get_config
from langgraph.typing import ContextT

from deerflow.agents.thread_state import ThreadState
from deerflow.memory.structured_memory_write_service import (
    StructuredMemoryWriteError,
    StructuredMemoryWriteService,
    format_write_success,
)


def _get_thread_id(runtime: ToolRuntime[ContextT, ThreadState]) -> str | None:
    if runtime.context and runtime.context.get("thread_id"):
        return str(runtime.context["thread_id"])
    cfg = getattr(runtime, "config", None) or {}
    tid = cfg.get("configurable", {}).get("thread_id")
    if tid:
        return str(tid)
    try:
        alt = get_config().get("configurable", {}).get("thread_id")
        return str(alt) if alt else None
    except RuntimeError:
        return None


@tool("structured_memory_write", parse_docstring=True)
def structured_memory_write_tool(
    runtime: ToolRuntime[ContextT, ThreadState],
    tier: Literal["raw", "distilled", "core"],
    title: str,
    content: str,
    tags: list[str] | None = None,
    source_thread_id: str | None = None,
    attachment_file_paths: list[str] | None = None,
    attachment_image_paths: list[str] | None = None,
    inline_web_urls: list[str] | None = None,
    raw_memory_ids: list[str] | None = None,
    distilled_memory_ids: list[str] | None = None,
    source_agent: str | None = None,
    user: str | None = None,
) -> str:
    """Persist one structured memory record (business knowledge) for cross-session reuse.

    Call this when you have stable, reusable knowledge worth saving (schemas, rules,
    troubleshooting notes, table logic). Do not use it for ephemeral chat filler.

    Tiers:
    - **raw**: requires ``source_thread_id`` at write time. If omitted, this tool attempts to use
      the current runtime LangGraph ``thread_id`` automatically. When recording material that
      belongs to another conversation, pass that historical thread id explicitly.
    - **distilled**: ``raw_memory_ids`` (≥1) required; ``source_thread_id`` is ignored.
    - **core**: ``distilled_memory_ids`` (≥1) required; ``source_thread_id`` is ignored.

    Body length is capped by ``structured_memory.write.max_content_length`` in config.

    Args:
        tier: Memory layer: raw, distilled, or core.
        title: Short label for the record (max 500 characters).
        content: Main text to store and embed.
        tags: Optional labels for filtering later.
        source_thread_id: For ``raw``: conversation thread id to anchor the record on.
        attachment_file_paths: Optional file paths cited in this raw memory.
        attachment_image_paths: Optional image paths cited in this raw memory.
        inline_web_urls: Optional URLs cited in this raw memory.
        raw_memory_ids: For distilled tier: ids of raw rows to link (required).
        distilled_memory_ids: For core tier: ids of distilled rows to link (required).
        source_agent: Optional logical writer name (defaults in repository).
        user: Optional user id (defaults in repository).
    """
    resolved_source_thread_id = source_thread_id
    if tier == "raw" and not (resolved_source_thread_id or "").strip():
        resolved_source_thread_id = _get_thread_id(runtime)
    if tier == "raw" and not (resolved_source_thread_id or "").strip():
        return (
            "structured_memory_write failed: tier raw requires a non-empty source_thread_id "
            "(the LangGraph thread this memory documents). If this write is for the current "
            "conversation, ensure runtime carries thread_id; for another session, pass that "
            "session's thread id explicitly."
        )

    service = StructuredMemoryWriteService()
    try:
        result = service.normalize_and_write(
            tier=tier,
            title=title,
            content=content,
            tags=tags,
            source_thread_id=resolved_source_thread_id,
            attachment_file_paths=attachment_file_paths,
            attachment_image_paths=attachment_image_paths,
            inline_web_urls=inline_web_urls,
            raw_memory_ids=raw_memory_ids,
            distilled_memory_ids=distilled_memory_ids,
            source_agent=source_agent,
            user=user,
        )
    except StructuredMemoryWriteError as exc:
        return f"structured_memory_write failed: {exc}"
    return format_write_success(result)
