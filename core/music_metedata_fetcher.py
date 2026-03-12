import logging
import os
import re
import time
import requests
from requests.exceptions import RequestException
from typing import Any, Dict, Optional, Tuple
from mutagen import File as MutagenFile


class MusicMetadataExtractor:
    def __init__(self, config: Dict[str, Any]):
        self.logger = logging.getLogger(__name__)
        self.url = "https://accounts.spotify.com/api/token"
        # Load Spotify credentials
        spotify_cfg = config.get("api_keys", {}).get("spotify", {}) or {}
        self.client_id = spotify_cfg.get("client_id", "")
        self.client_secret = spotify_cfg.get("client_secret", "")
        self.spotify_token = self._get_spotify_token()

    def _get_spotify_token(self, retries=5, delay=2) -> str:
        """
        Fetch a Spotify API token with retry + backoff to handle 502/5xx errors.
        """

        for attempt in range(1, retries + 1):
            try:
                resp = requests.post(
                    url=self.url,
                    data={"grant_type": "client_credentials"},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    auth=(self.client_id, self.client_secret),
                    timeout=10,
                )
                resp.raise_for_status()
                return resp.json()["access_token"]

            except RequestException as e:
                self.logger.warning(
                    f"Spotify token request failed (attempt {attempt}/{retries}): {e}"
                )

                if attempt < retries:
                    sleep_time = delay * attempt  # exponential backoff
                    time.sleep(sleep_time)
                else:
                    raise

    def _spotify_search(
            self, title: str, artist: Optional[str] = None
    ) -> Optional[Tuple[str, str, str, str, str]]:
        """
        Search Spotify for a track.
        Returns (artist, album, title, year, cover_url).
        """
        headers = {"Authorization": f"Bearer {self.spotify_token}"}
        query = f'track:"{title}"'
        if artist:
            query += f' artist:"{artist}"'

        url = "https://api.spotify.com/v1/search"
        try:
            resp = requests.get(
                url,
                params={"q": query, "type": "track", "limit": 1},
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 401:  # token expired
                if self.logger:
                    self.logger.info("Refreshing Spotify token...")
                self.spotify_token = self._get_spotify_token()
                return self._spotify_search(title, artist)

            resp.raise_for_status()
            results = resp.json()
            items = results.get("tracks", {}).get("items", [])
            if not items:
                return None

            track = items[0]
            track_artist = track["artists"][0]["name"]
            track_album = track["album"]["name"]
            track_title = track["name"]
            release_year = track["album"]["release_date"].split("-")[0]
            cover_url = None
            if track["album"]["images"]:
                # Pick the medium size (Spotify usually provides 640, 300, 64 px)
                cover_url = track["album"]["images"][0]["url"]

            return track_artist, track_album, track_title, release_year, cover_url
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Spotify query failed for {title}: {e}")
        return None

    def _parse_tags(self, filepath: str, parent_folder: Optional[str]):
        """Extract metadata from file tags."""
        try:
            audio = MutagenFile(filepath, easy=True)
            if not audio:
                return None
            artist = audio.get("artist", [None])[0]
            album = audio.get("album", [None])[0] or parent_folder
            title = audio.get("title", [None])[0]
            track_num = None
            if "tracknumber" in audio:
                try:
                    track_num = int(str(audio["tracknumber"][0]).split("/")[0])
                except Exception:
                    pass
            if artist or album or title:
                return artist, album, track_num, title
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Mutagen read failed: {e}")
        return None

    def _parse_filename(self, filepath: str, parent_folder: Optional[str]):
        """Infer metadata from filename patterns."""
        base = os.path.splitext(os.path.basename(filepath))[0]

        # Album-style: NN - Title
        match = re.match(r"(\d{1,2})[\s\-.]+(.+)", base)
        if match:
            return None, parent_folder, int(match.group(1)), match.group(2).strip()

        # Single-style: Artist - Title
        match = re.match(r"(.+?)[\s\-_]+-\s+(.+)", base)
        if match:
            return match.group(1).strip(), parent_folder, None, match.group(2).strip()

        # Clean junk tokens
        junk_patterns = [r"\bDJMaza\b", r"\b128\b", r"\bmp3\b", r"\d{6,}"]
        for pat in junk_patterns:
            base = re.sub(pat, "", base, flags=re.IGNORECASE)
        clean_title = re.sub(r"[._\-]+", " ", base).strip()
        return None, parent_folder, None, clean_title

    def extract_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Main extractor function with safe fallback defaults."""
        filename = media_info.get("filename")
        guessit_info = dict(media_info.get("guessit_info", {}))
        filepath = guessit_info.get("original_file_path")
        parent_folder = os.path.basename(os.path.dirname(filepath))

        # Step 1: Try tags → fallback to filename
        info = self._parse_tags(filepath, parent_folder) or self._parse_filename(
            filepath, parent_folder
        )
        artist, album, track_num, track_title = info

        # Step 2: Spotify search (with anime fallback)
        cover_url = None
        release_year = None
        if track_title:
            sp_info = self._spotify_search(track_title, artist)
            if not sp_info:  # try anime-boosted query
                sp_info = self._spotify_search(f"{track_title} anime", artist)
            if sp_info:
                artist, album, track_title, release_year, cover_url = sp_info

        # Step 3: Fallback defaults
        artist = artist or "Unknown Artist"
        album = album or (parent_folder or "Unknown Album")
        track_title = track_title or os.path.splitext(os.path.basename(filepath))[0]
        track_num = track_num if track_num is not None else 0
        release_year = release_year or "Unknown Year"
        # keep cover_url as None if not found (better than "No Cover")
        cover_url = cover_url or None

        # Step 4: Ensure search_title & filename are never None
        search_title = (
                track_title
                or guessit_info.get("search_title")
                or os.path.splitext(os.path.basename(filepath))[0]
                or "Unknown Title"
        )
        original_filename = guessit_info.get("original_filename") or filename or "Unknown File"

        return {
            "artist": artist,
            "album": album,
            "title": track_title,
            "track": track_num,
            "year": release_year,
            "cover_url": cover_url,
            "search_title": search_title,
            "original_filename": original_filename,
            "filepath": filepath,
            "source": "mutagen+spotify",
        }
