"""Local cache of downloaded payloads and parsed tables.

Two layers under one root directory:

- ``raw/`` mirrors the nemweb path of every downloaded file, keeping full
  provenance (a parser fix never requires a re-download).
- ``parquet/`` stores each parsed ``(payload, table)`` pair so repeated
  reads skip zip extraction and C/I/D parsing entirely.
"""

from __future__ import annotations

import logging
import time
import urllib.parse
from pathlib import Path

import pandas as pd
import requests

from nemdatatools.cid import parse_cid_zip
from nemdatatools.listing import make_session

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = Path.home() / ".nemdatatools"

# Seconds between download requests; nemweb is a shared public service.
POLITE_DELAY = 0.5


class Cache:
    """Raw-download and parsed-table cache rooted at one directory."""

    def __init__(
        self,
        root: Path | str = DEFAULT_CACHE_DIR,
        session: requests.Session | None = None,
    ) -> None:
        """Create a cache.

        Args:
            root: Cache root directory; created on first use.
            session: Optional shared HTTP session.

        """
        self.root = Path(root).expanduser()
        self.session = session or make_session()
        self._last_download = 0.0

    def _raw_path(self, url: str) -> Path:
        """Local mirror path of a nemweb URL under raw/."""
        path = urllib.parse.unquote(urllib.parse.urlparse(url).path)
        return self.root / "raw" / path.lstrip("/")

    def download(self, url: str) -> Path:
        """Return a local copy of ``url``, downloading only when missing.

        Args:
            url: Absolute nemweb file URL.

        Returns:
            Path of the cached raw file.

        Raises:
            requests.HTTPError: If the download fails.

        """
        target = self._raw_path(url)
        if target.exists() and target.stat().st_size > 0:
            return target
        wait = POLITE_DELAY - (time.monotonic() - self._last_download)
        if wait > 0:
            time.sleep(wait)
        logger.info("downloading %s", url)
        response = self.session.get(url, timeout=600)
        self._last_download = time.monotonic()
        if response.status_code == 403:
            # nemweb's CDN intermittently answers 403 for files that exist;
            # one spaced retry recovers most of them.
            time.sleep(2.0)
            response = self.session.get(url, timeout=600)
            self._last_download = time.monotonic()
        response.raise_for_status()
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".part")
        tmp.write_bytes(response.content)
        tmp.replace(target)
        return target

    def load_table(self, url: str, cid_key: tuple[str, str]) -> pd.DataFrame:
        """Fetch ``url`` and return one table from its C/I/D payload.

        Results are memoized as Parquet per ``(payload, table)``; other
        tables found while parsing are memoized too, so multi-table
        payloads (e.g. TradingIS) parse once.

        Args:
            url: Absolute nemweb zip URL.
            cid_key: ``(component, table)`` to extract.

        Returns:
            The table's rows from this payload; empty when the payload
            carries no such segment.

        """
        marker = self._parquet_path(url, cid_key)
        if marker.exists():
            return pd.read_parquet(marker)
        raw = self.download(url)
        # Segments sharing (component, table) — e.g. two schema versions in
        # one payload — merge into one frame per key before writing, so each
        # parse pass overwrites its parquet files exactly once.
        merged: dict[tuple[str, str], list[pd.DataFrame]] = {}
        for table in parse_cid_zip(raw):
            key = (table.key.component, table.key.table)
            merged.setdefault(key, []).append(table.frame)
        # Memoize the requested key even when absent, so misses do not
        # re-parse forever.
        merged.setdefault(cid_key, [])
        for key, frames in merged.items():
            frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            path = self._parquet_path(url, key)
            path.parent.mkdir(parents=True, exist_ok=True)
            # Write-then-replace like download(): an interrupted direct
            # write would leave a corrupt marker that poisons every
            # future cache hit for this (payload, table).
            tmp = path.with_suffix(path.suffix + ".part")
            frame.to_parquet(tmp)
            tmp.replace(path)
        return pd.read_parquet(marker)

    def _parquet_path(self, url: str, cid_key: tuple[str, str]) -> Path:
        """Parquet memo path for one (payload, table) pair."""
        raw = self._raw_path(url)
        rel = raw.relative_to(self.root / "raw")
        return (
            self.root
            / "parquet"
            / rel.parent
            / f"{rel.name}.{cid_key[0]}.{cid_key[1]}.parquet"
        )
