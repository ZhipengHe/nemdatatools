"""Directory-listing access for nemweb.com.au.

Every nemweb data area (Reports CURRENT, Reports ARCHIVE, the MMSDM data
archive) is exposed as IIS directory-listing pages. The listing HTML is not
uniform across areas — Data_Archive pages give directory links a trailing
slash, while Reports pages mark directories only with a ``<dir>`` token in
the surrounding text — so all remote file discovery goes through this one
parser instead of constructing filenames, which also absorbs AEMO's
filename-era changes.
"""

from __future__ import annotations

import logging
import urllib.parse
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nemweb.com.au"

_RETRY = Retry(
    total=5,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "HEAD"),
)


@dataclass(frozen=True)
class ListingEntry:
    """One file or subdirectory in a nemweb directory listing.

    Attributes:
        name: Final path segment with percent-encoding decoded, so archive
            filenames read as published (e.g. ``PUBLIC_ARCHIVE#TABLE#...``).
        url: Absolute HTTPS URL of the entry.
        is_dir: True when the entry is a subdirectory.

    """

    name: str
    url: str
    is_dir: bool


def make_session() -> requests.Session:
    """Create an HTTP session with retry/backoff suited to nemweb."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = (
        "nemdatatools (+https://github.com/ZhipengHe/nemdatatools)"
    )
    return session


def list_directory(
    url: str,
    session: requests.Session | None = None,
    timeout: float = 60.0,
) -> list[ListingEntry]:
    """Fetch and parse one nemweb directory listing.

    Args:
        url: Absolute URL of the directory (with or without trailing slash).
        session: Optional shared session; one is created if omitted.
        timeout: Request timeout in seconds.

    Returns:
        Entries in listing order, excluding the parent-directory link.

    Raises:
        requests.HTTPError: If the listing page cannot be fetched.

    """
    if not url.endswith("/"):
        url += "/"
    sess = session or make_session()
    response = sess.get(url, timeout=timeout)
    response.raise_for_status()
    return parse_listing_html(response.text, base_url=url)


def parse_listing_html(html: str, base_url: str) -> list[ListingEntry]:
    """Parse IIS directory-listing HTML into entries.

    Handles both HREF quoting styles found across nemweb areas.

    Args:
        html: Raw listing page HTML.
        base_url: URL of the listing page, used to absolutise hrefs.

    Returns:
        Entries in listing order, excluding the parent-directory link.

    """
    if not base_url.endswith("/"):
        base_url += "/"
    entries: list[ListingEntry] = []
    for anchor in BeautifulSoup(html, "html.parser").find_all("a"):
        href = anchor.get("href")
        # bs4 types attribute values as str-or-list (multi-valued attrs);
        # href is never legitimately a list, so skip anything else.
        if not href or not isinstance(href, str):
            continue
        text = anchor.get_text(strip=True)
        if text.lower() == "[to parent directory]":
            continue
        absolute = urllib.parse.urljoin(base_url, href)
        path = urllib.parse.urlparse(absolute).path
        # Drop parent/self links that appear without the marker text. IIS
        # paths are case-insensitive, so compare casefolded.
        base_path = urllib.parse.urlparse(base_url).path
        if not path.casefold().startswith(base_path.casefold()) or (
            path.casefold().rstrip("/") == base_path.casefold().rstrip("/")
        ):
            continue
        # Data_Archive listings give directories a trailing slash; Reports
        # listings instead put a "<dir>" token in the text before the link.
        preceding = anchor.previous_sibling
        preceding_text = preceding if isinstance(preceding, str) else ""
        is_dir = path.endswith("/") or "<dir>" in preceding_text
        name = urllib.parse.unquote(path.rstrip("/").rsplit("/", 1)[-1])
        entries.append(ListingEntry(name=name, url=absolute, is_dir=is_dir))
    return entries
