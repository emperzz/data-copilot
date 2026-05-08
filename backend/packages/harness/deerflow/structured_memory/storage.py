"""File-based storage for enterprise structured memory."""

import logging
import os
import threading
import uuid
from pathlib import Path

from deerflow.config.paths import get_paths
from deerflow.config.structured_memory_config import get_structured_memory_config

logger = logging.getLogger(__name__)

MEMORY_ROOT_DIRNAME = "structured_memory"

_lock = threading.Lock()


def _resolve_storage_root() -> Path:
    """Resolve the root directory for structured memory files."""
    config = get_structured_memory_config()
    if config.storage_path:
        p = Path(config.storage_path)
        if not p.is_absolute():
            p = get_paths().base_dir / p
        return p
    return get_paths().base_dir / MEMORY_ROOT_DIRNAME


class StructuredMemoryStore:
    """Manages file I/O for the structured memory directory tree."""

    def __init__(self) -> None:
        self._root: Path | None = None

    @property
    def root(self) -> Path:
        if self._root is None:
            self._root = _resolve_storage_root()
        return self._root

    def ensure_directories(self) -> None:
        """Create the base directory structure if it doesn't exist."""
        dirs = [
            self.root,
            self.root / "facts" / "business",
            self.root / "facts" / "technical",
            self.root / "facts" / "warehouse",
            self.root / "tasks",
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    def resolve_path(self, relative_path: str) -> Path:
        """Resolve a relative path within the memory root.

        Validates that the resolved path stays within the memory root.
        """
        normalized = os.path.normpath(relative_path)
        target = (self.root / normalized).resolve()
        if not target.is_relative_to(self.root):
            raise ValueError(f"Path traversal rejected: {relative_path!r}")
        return target

    def read_file(self, relative_path: str) -> str:
        """Read a memory file by relative path."""
        target = self.resolve_path(relative_path)
        if not target.exists():
            raise FileNotFoundError(f"Memory file not found: {relative_path}")
        return target.read_text(encoding="utf-8")

    def write_file(self, relative_path: str, content: str) -> None:
        """Write (create or overwrite) a memory file atomically."""
        target = self.resolve_path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: temp file + rename
        tmp_path = target.parent / f".{uuid.uuid4().hex}.tmp"
        try:
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.replace(target)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def list_dir(self, relative_path: str = "", depth: int = 2) -> str:
        """List directory contents in tree format.

        Args:
            relative_path: Relative path within memory root. Empty = root level.
            depth: Maximum directory depth to traverse.

        Returns:
            Tree-formatted string of directory contents.
        """
        target = self.resolve_path(relative_path) if relative_path else self.root
        if not target.exists():
            raise FileNotFoundError(f"Directory not found: {relative_path}")

        lines: list[str] = []
        for dirpath, dirnames, filenames in os.walk(target):
            current_depth = len(Path(dirpath).relative_to(target).parts)
            if current_depth >= depth:
                dirnames.clear()

            indent = "  " * current_depth
            folder_name = Path(dirpath).name if current_depth > 0 else MEMORY_ROOT_DIRNAME
            if current_depth == 0:
                lines.append(folder_name + "/")
            else:
                lines.append(f"{indent}{folder_name}/")

            for fname in sorted(filenames):
                lines.append(f"{indent}  {fname}")

        return "\n".join(lines)

    def file_exists(self, relative_path: str) -> bool:
        """Check whether a memory file exists."""
        return self.resolve_path(relative_path).exists()


# Global singleton with thread-safe initialization
_store: StructuredMemoryStore | None = None


def get_structured_memory_store() -> StructuredMemoryStore:
    """Return the global StructuredMemoryStore singleton (thread-safe)."""
    global _store
    if _store is None:
        with _lock:
            if _store is None:
                _store = StructuredMemoryStore()
    return _store
