"""Shared base for source plugins (kept separate to avoid circular imports)."""
from typing import Any, Iterator


class RejectRecord(Exception):
    """Raise inside transform() to send ONE record to file_reject_log."""


class SourcePlugin:
    """One per source. Wraps your EXISTING decoder + transformation."""

    def setup(self) -> None:
        """Runs ONCE PER WORKER: build decoder, load lookups."""

    def decode(self, path: str) -> Iterator[Any]:
        """Open `path` (gs://...) yourself and yield decoded records."""
        raise NotImplementedError

    def transform(self, record: Any):
        """dict | list[dict] | None. raise RejectRecord(...) for a bad record."""
        raise NotImplementedError
