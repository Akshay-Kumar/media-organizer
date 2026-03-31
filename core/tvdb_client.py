import logging
from typing import Dict, Any, Optional, List
import requests
from datetime import datetime, timedelta
import re
from core.tvdb_v4_official import TVDB


class TVDBClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://api4.thetvdb.com/v4"
        self.api_key = config['api_keys'].get('tvdb', '')
        self.token = None
        self.token_expiry = None
        self.tvdb_v4_official = TVDB(apikey=self.api_key)

    def _authenticate(self) -> bool:
        """Authenticate with TVDB API v4"""
        if self.token and self.token_expiry and datetime.now() < self.token_expiry:
            return True

        try:
            auth_url = f"{self.base_url}/login"
            auth_data = {"apikey": self.api_key}

            response = requests.post(auth_url, json=auth_data, timeout=30)
            response.raise_for_status()

            auth_response = response.json()
            if auth_response.get("status") == "success":
                self.token = auth_response["data"]["token"]
                self.token_expiry = datetime.now() + timedelta(hours=23)  # Tokens last 24h
                return True

        except Exception as e:
            self.logger.error(f"TVDB authentication failed: {e}")

        return False

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make authenticated request to TVDB API"""
        if not self._authenticate():
            return None

        try:
            url = f"{self.base_url}/{endpoint}"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }

            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            self.logger.warning(f"TVDB API request failed for {endpoint}: {e}")
            return None

    def search_series(self, query: str, language: str = "eng", limit: int = 1) -> Optional[Dict[str, Any]]:
        """Search for a single best match series (TVDB v4 supports limit + language)."""
        result = self._make_request(
            "search",
            {"query": query, "type": "series", "language": language, "limit": limit}
        )
        if result and result.get("data"):
            return result["data"]
        return None

    def get_series_details(self, series_id: int) -> Optional[Dict[str, Any]]:
        """Get series details by ID"""
        result = self._make_request(f"series/{series_id}")
        return result.get("data") if result and result.get("data") else None

    def get_episodes(
            self,
            series_id: int,
            season: int = None,
            episode_number: int = None,
            page: int = 1
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get episodes for a series (paginated, can filter by season and episode number).
        Matches TVDB v4 official episodes endpoint:
        /series/{series_id}/episodes/official?page=1&season=4&episodeNumber=1
        """
        endpoint = f"series/{series_id}/episodes/official"
        params: Dict[str, int] = {"page": page}
        if season is not None:
            params["season"] = season
        if episode_number is not None:
            params["episodeNumber"] = episode_number

        result = self._make_request(endpoint, params)
        return result.get("data") if result and result.get("data") else None

    def get_all_episodes(
            self,
            series_id: int,
            page: int = 1
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get all episodes for a series (paginated, can filter by season and episode number).
        Matches TVDB v4 official episodes endpoint:
        /series/{series_id}/extended?meta=episodes&short=true
        """
        endpoint = f"series/{series_id}/extended?meta=episodes&short=true"
        params: Dict[str, int] = {"page": page}
        result = self._make_request(endpoint, params)
        return result.get("data") if result and result.get("data") else None

    def get_episode_details(self, episode_id: int) -> Optional[Dict[str, Any]]:
        """Get specific episode details (with translations if available)."""
        result = self._make_request(f"episodes/{episode_id}")
        if result and result.get("data"):
            ep_data = result["data"]

            # Try to fetch English translation for episode title/overview
            translation = self._make_request(f"episodes/{episode_id}/translations/eng")
            if translation and translation.get("data"):
                ep_data["name"] = translation["data"].get("name", ep_data.get("name"))
                ep_data["overview"] = translation["data"].get("overview", ep_data.get("overview"))

            return ep_data
        return None

    def search_episode(self, series_name: str, season: int, episode: int) -> Optional[Dict[str, Any]]:
        """Find an episode by series name, season, and episode number."""
        page = 1
        limit = 1
        lang = 'eng'
        series_name = self.remove_year_from_title(series_name)
        series_results = self.search_series(series_name, language=lang, limit=limit)
        if not series_results:
            return None

        # Normalize for best match
        query_norm = series_name.lower().replace(":", "").strip()

        # Pick the best matching series
        best_match = None
        for result in series_results:
            slug = result.get("slug", "").lower().replace(":", "").strip() if result.get("slug") else None
            title = result.get("name", "").lower().replace(":", "").strip() if result.get("name") else None
            aliases = list(result.get("aliases")) if result.get("aliases") else None

            if slug:
                if slug == query_norm:
                    best_match = result
                    break

            if title:
                if title == query_norm:
                    best_match = result
                    break

            if aliases:
                for alias in aliases:
                    alias = alias.lower().replace(":", "").strip()
                    if alias == query_norm:
                        best_match = result
                        break

        if not best_match:
            return None

        series_id = best_match.get("tvdb_id")  # ✅ fixed from tvdb_id → id
        if not series_id:
            return None

        episodes_data = self.get_episodes(series_id, season, episode, page=page)
        series_title = series_name
        episodes = list(episodes_data.get("episodes", None)) if episodes_data else None

        if episodes and len(episodes) > 0:
            for ep in episodes:
                if ep.get("seasonNumber") == season and ep.get("number") == episode:
                    ep_data = self.get_episode_details(ep["id"])
                    season_number = ep_data['seasonNumber']
                    episode_number = ep_data['number']
                    episode_title = ep_data['name']
                    episode_overview = ep_data['overview']
                    return {
                        'title': series_title,
                        'season': season_number,
                        'episode': episode_number,
                        'episode_title': episode_title,
                        'overview': episode_overview,
                        'tvdb_id': series_id,
                        'image': ep_data.get("image"),
                        'source': 'tvdb'
                    }

        return None

    def search_episode2(self, tvdb_id: str, season: int, episode: int) -> Optional[Dict[str, Any]]:
        """Find an episode by series tvdb_id, season, and episode number."""
        page = 1
        limit = 1
        lang = 'eng'
        series_results = self.tvdb_v4_official.get_series(id=int(tvdb_id))
        if not series_results:
            return None

        series_title = series_results.get("name") or series_results.get("slug")
        episodes_data = self.tvdb_v4_official.get_series_episodes(int(tvdb_id))
        episodes = list(episodes_data.get("episodes", None)) if episodes_data else None

        if episodes and len(episodes) > 0:
            for ep in episodes:
                if ep.get("seasonNumber") == season and ep.get("number") == episode:
                    ep_data = self.get_episode_details(ep["id"])
                    season_number = ep_data['seasonNumber']
                    episode_number = ep_data['number']
                    episode_title = ep_data['name']
                    episode_overview = ep_data['overview']
                    year = ep_data.get('aired', '')[:4] if ep_data.get('year') else None
                    return {
                        'title': series_title,
                        'season': season_number,
                        'episode': episode_number,
                        'episode_title': episode_title,
                        'overview': episode_overview,
                        'year': year,
                        'tvdb_id': tvdb_id,
                        'image': ep_data.get("image"),
                        'source': 'tvdb'
                    }

        return None

    def search_episode_by_episode_number(self, series_name: str, episode: int) -> Optional[Dict[str, Any]]:
        """Find an episode by series name and absolute episode number."""
        page = 1
        max_pages = 1
        limit = 10
        lang = 'eng'
        series_name = self.remove_year_from_title(series_name)
        series_results = self.search_series(series_name, language=lang, limit=limit)
        if not series_results:
            return None

        # Normalize for best match
        query_norm = series_name.lower().replace(":", "").strip()

        # 1. Pick the best matching series
        best_match = None
        for result in series_results:
            slug = result.get("slug", "").lower().replace(":", "").strip() if result.get("slug") else None
            title = result.get("name", "").lower().replace(":", "").strip() if result.get("name") else None

            if slug:
                if slug == query_norm:
                    best_match = result
                    break

            if title:
                if title == query_norm:
                    best_match = result
                    break

        # 2. Alias matching
        if not best_match:
            for result in series_results:
                aliases = result.get("aliases", []) if result.get("aliases") else None

                if aliases:
                    for alias in aliases:
                        alias_norm = str(alias).lower().replace(":", "").strip()
                        if alias_norm == query_norm:
                            best_match = result
                            break

        if not best_match:
            return None

        series_id = best_match.get("tvdb_id")  # ✅ fixed from tvdb_id → id
        if not series_id:
            return None

        while page <= max_pages:
            episodes_data = self.get_all_episodes(series_id, page=page)
            episodes = list(episodes_data.get("episodes")) if episodes_data else None
            series_title = series_name

            if episodes and len(episodes) > 0:
                for ep in episodes:
                    if ep['absoluteNumber'] == episode:
                        ep_data = self.get_episode_details(ep["id"])
                        season_number = ep['seasonNumber']
                        episode_number = ep['number']
                        episode_title = ep_data['name']
                        episode_overview = ep_data['overview']
                        return {
                            'title': series_title,
                            'season': season_number,
                            'episode': episode_number,
                            'episode_title': episode_title,
                            'overview': episode_overview,
                            'tvdb_id': series_id,
                            'image': ep_data.get("image"),
                            'source': 'tvdb'
                        }
                page = page + 1

        return None

    def remove_year_from_title(self, title: str) -> str:
        # Removes patterns like "(2023)" or "(1999)"
        return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
