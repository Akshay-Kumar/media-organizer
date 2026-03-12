import requests
import hashlib
import xml.etree.ElementTree as ET
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta


class AniDBClient:
    """
    AniDB HTTP API client (XML).
    Reads directly from config.yaml under api_keys.anidb.

    Example config:
    api_keys:
      anidb:
        username: ""
        password: ""
        is_pass_hashed: False
        hash_type: "md5"
        client: "myanimeapp"
        version: "1"
    """

    def __init__(self, config: Dict[str, Any]):
        self.logger = logging.getLogger(__name__)

        # Load AniDB section
        anidb_cfg = config.get("api_keys", {}).get("anidb", {}) or {}

        # self.base_url = "https://api.anidb.net:9001/httpapi"
        self.base_url = "https://api.anidb.net/httpapi"
        self.username = anidb_cfg.get("username", "")
        self.client = anidb_cfg.get("client")
        self.clientver = str(anidb_cfg.get("version"))
        self.protover = "1"

        # Password handling
        self.password_is_hashed = bool(anidb_cfg.get("is_pass_hashed", False))
        self.password_hash_type = str(anidb_cfg.get("hash_type", "md5")).lower()
        self._raw_password = str(anidb_cfg.get("password", "") or "")
        self._prehashed = None  # not in YAML
        self._password_hash = None

        # Authentication caching
        self._authenticated = False
        self._auth_checked_at: Optional[datetime] = None
        self._auth_cache_ttl = timedelta(hours=23)
        self._auth_test_title = "Naruto"

    # -------------------------
    # Utility / auth helpers
    # -------------------------
    def _compute_password_hash(self) -> str:
        """Return password hash string."""
        if self._password_hash:
            return self._password_hash

        if self._prehashed:
            self._password_hash = str(self._prehashed)
            return self._password_hash

        pwd = self._raw_password
        if self.password_is_hashed:
            self._password_hash = pwd
            return self._password_hash

        if self.password_hash_type == "sha1":
            self._password_hash = hashlib.sha1(pwd.encode("utf-8")).hexdigest()
        else:  # default md5
            self._password_hash = hashlib.md5(pwd.encode("utf-8")).hexdigest()

        return self._password_hash

    def authenticate(self, force: bool = False) -> bool:
        """Validate credentials with a small test query."""
        now = datetime.utcnow()
        if not force and self._authenticated and self._auth_checked_at and (now - self._auth_checked_at) < self._auth_cache_ttl:
            return True

        if not self.username or not self._raw_password:
            self.logger.warning("AniDB credentials missing.")
            self._authenticated = False
            return False

        try:
            root = self._make_request({"request": "anime", "title": self._auth_test_title})
            if root is None:
                self._authenticated = False
            elif root.find("error") is not None:
                self.logger.warning(f"AniDB auth error: {ET.tostring(root, encoding='utf-8')}")
                self._authenticated = False
            else:
                self._authenticated = True
                self._auth_checked_at = now
        except Exception as e:
            self.logger.warning(f"AniDB authenticate failed: {e}")
            self._authenticated = False

        return self._authenticated

    # -------------------------
    # Core request
    # -------------------------
    def _make_request(self, params: Dict[str, Any]) -> Optional[ET.Element]:
        pw_hash = self._compute_password_hash()
        merged = {
            "client": self.client,
            "clientver": self.clientver,
            "protover": self.protover,
            "user": self.username,
            "pass": pw_hash,
        }
        merged.update(params)

        try:
            resp = requests.get(self.base_url, params=merged, timeout=30, verify=False)
            resp.raise_for_status()
            if not resp.text.strip():
                return None
            return ET.fromstring(resp.text.strip())
        except Exception as e:
            self.logger.warning(f"AniDB request failed: {e}")
            return None

    # -------------------------
    # High-level methods
    # -------------------------
    def search_anime(self, title: str) -> Optional[List[Dict[str, Any]]]:
        if not self.authenticate():
            return None

        root = self._make_request({"request": "anime", "title": title})
        if root is None:
            return None

        results = []
        for anime_elem in root.findall(".//anime"):
            aid = anime_elem.attrib.get("id") or anime_elem.findtext("id") or anime_elem.findtext("aid")
            results.append({
                "aid": aid,
                "title": anime_elem.findtext("title") or anime_elem.findtext("main_title") or "",
                "type": anime_elem.findtext("type"),
                "startdate": anime_elem.findtext("startdate"),
                "enddate": anime_elem.findtext("enddate"),
                "description": anime_elem.findtext("description") or ""
            })
        return results if results else None

    def get_episodes(self, anime_id: str) -> Optional[List[Dict[str, Any]]]:
        if not self.authenticate():
            return None

        root = self._make_request({"request": "anime", "aid": anime_id})
        if root is None:
            return None

        episodes: List[Dict[str, Any]] = []
        for ep in root.findall(".//episode"):
            episodes.append({
                "id": ep.attrib.get("id"),
                "epno": ep.findtext("epno"),
                "title": ep.findtext("title"),
                "airdate": ep.findtext("airdate"),
                "summary": ep.findtext("summary"),
                "length": ep.findtext("length"),
                "rating": ep.findtext("rating"),
                "arc": ep.findtext("arc") or ep.findtext("season")
            })
        return episodes if episodes else None

    def search_episode(self, series_name: str, season: int, episode: int) -> Optional[Dict[str, Any]]:
        candidates = self.search_anime(series_name)
        if not candidates:
            return None

        best = next((c for c in candidates if (c.get("title") or "").lower() == series_name.lower()), candidates[0])
        anime_id = best.get("aid")
        episodes = self.get_episodes(anime_id)
        if not episodes:
            return None

        # Match season + episode if arc present
        for ep in episodes:
            if str(ep.get("arc")) == str(season) and str(ep.get("epno")) == str(episode):
                return {"title": best.get("title"), "anime_id": anime_id, **ep}

        # Fallback: epno only
        for ep in episodes:
            if str(ep.get("epno")) == str(episode):
                return {"title": best.get("title"), "anime_id": anime_id, **ep}

        return None
