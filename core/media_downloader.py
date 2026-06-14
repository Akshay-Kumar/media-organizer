import base64
import logging
import zlib
from pathlib import Path
from typing import Dict, Any, Optional
import time
import requests
from opensubtitlescom import OpenSubtitles
from tmdbv3api import List


class MediaDownloader:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()

        if self.config['download'].get('subtitles'):
            opensub_conf = self.config['download'].get('opensubtitles', {})
            if all(k in opensub_conf for k in ('username', 'password', 'api_key')):
                # Set global credentials for the OpenSubtitles
                self.api_key = opensub_conf["api_key"]
                self.user_agent = opensub_conf.get('user_agent', 'media-organizer 1.0')
                self.client = OpenSubtitles(self.user_agent, self.api_key)
                self.username = opensub_conf['username']
                self.password = opensub_conf['password']
                self.token = None
                self.token_expiry = 0
                self.login()
            else:
                self.logger.warning("OpenSubtitles credentials not set.")

    def login(self):
        """Login only when token is missing or expired."""
        now = time.time()
        if self.token and now < self.token_expiry:
            return  # already logged in with valid token

        resp = self.client.login(self.username, self.password)
        if not resp or "token" not in resp:
            raise RuntimeError(f"OpenSubtitles login failed: {resp}")

        self.token = resp["token"]
        self.token_expiry = resp.get("exp", now + 23 * 3600)  # fallback: ~23h
        self.logger.info(f"OpenSubtitles login OK, token expires at {self.token_expiry}")

    def download_artwork(self, metadata: Dict[str, Any], media_type: str, destination: Path) -> Dict[str, str]:
        """
        Download artwork (poster, backdrop) to the same folder as the media file.
        Supports movies (TMDb) and TV shows (TVDB).
        """
        artwork_paths = {}
        if not self.config['download']['artwork']:
            return artwork_paths

        try:
            # --- Movie (TMDb) ---
            if media_type == 'movie':
                # Poster
                if metadata.get('poster_path'):
                    poster_url = f"https://image.tmdb.org/t/p/{self.config['download']['artwork_sizes']['poster']}{metadata['poster_path']}"
                    poster_path = self._download_image(poster_url,
                                                       str(destination.parent) + "/" + str(
                                                           destination.stem) + "_poster" + ".jpg")
                    if poster_path:
                        artwork_paths['poster'] = str(poster_path)

                # Backdrop
                if metadata.get('backdrop_path'):
                    backdrop_url = f"https://image.tmdb.org/t/p/{self.config['download']['artwork_sizes']['backdrop']}{metadata['backdrop_path']}"
                    backdrop_path = self._download_image(backdrop_url,
                                                         str(destination.parent) + "/" + str(
                                                             destination.stem) + "_backdrop" + ".jpg")
                    if backdrop_path:
                        artwork_paths['backdrop'] = str(backdrop_path)

            # --- TV Show (TVDB) ---
            elif media_type in ['tv_show', 'anime']:
                # Series poster
                # series_image = metadata.get('series', {}).get('image')
                series_image = metadata.get('image')
                if series_image:
                    poster_path = self._download_image(series_image,
                                                       str(destination.parent) + "/" + str(destination.stem) + ".jpg")
                    if poster_path:
                        artwork_paths['poster'] = str(poster_path)

        except Exception as e:
            self.logger.error(f"Error downloading artwork: {e}")

        return artwork_paths

    # --- Helper function to decompress subtitles ---
    def decompress_subtitle(self, data: str) -> str:
        """Convert OpenSubtitles base64 + zlib compressed subtitles to string"""
        raw = zlib.decompress(base64.b64decode(data), 16 + zlib.MAX_WBITS)
        return raw.decode('utf-8', errors='ignore')

    def download_subtitles(self, destination: Path, metadata: Dict[str, Any]):
        if not self.config['download']['subtitles']:
            return None

        languages = self.config["download"].get(
            "subtitle_languages",
            ["en"]
        )

        media_type = metadata.get("media_type")
        subtitle_paths = []
        query = ""
        year = None
        episode_number = None
        season_number = None
        if media_type and media_type == "movie":
            query = metadata.get("title")
            if metadata.get("year"):
                year = metadata.get("year")
        elif media_type and media_type in ["anime", "tv_show"]:
            query = metadata.get("title")
            season_number = metadata.get("season")
            episode_number = metadata.get("episode")

        try:
            for language in languages:
                results = self.download_subtitle(
                    query=query,
                    media_type=media_type,
                    episode_number=episode_number,
                    season_number=season_number,
                    year=year,
                    language=language,
                    destination=destination
                )
                if results:
                    self.logger.info(f"Downloaded subtitles: {results}")
                    subtitle_paths.extend(results)
                else:
                    self.logger.info(f"No subtitles found for {destination.stem}")

        except Exception as e:
            self.logger.error(f"Error downloading subtitles for {destination.name}: {e}")
            return None

        return subtitle_paths

    def download_subtitle(self, query: str, media_type: str, destination: Path, season_number: int = None,
                          episode_number: int = None,
                          year: int = None,
                          language: str = "en"):
        """
        Search and download the first subtitle for a given IMDb ID
        """
        response = {}
        subtitle_paths = []
        if media_type in ["tv_show", "anime"]:
            response = self.client.search(type=media_type, query=query, season_number=season_number,
                                          episode_number=episode_number,
                                          languages=language)
        elif media_type == "movie":
            response = self.client.search(type=media_type, query=query, year=year, languages=language)

        subtitles = response.to_dict().get('data', [])
        max_count = self.config["download"].get(
            "max_subtitles_per_language",
            3
        )
        selected_subs = subtitles[:max_count]

        if not selected_subs:
            print(f"No subtitles found for IMDb ID {query}")
            return None

        for sub in selected_subs:
            # pick the subtitle file_id
            file_id = sub.file_id
            file_name = sub.file_name
            language = sub.language or language

            # Download subtitle
            subtitle_data = self.client.download(file_id)

            # Save to disk
            output_folder = str(destination.parent)
            output_path = Path(output_folder) / (Path(file_name).stem + "." + language + ".srt")
            with open(output_path, "wb") as srt:
                srt.write(subtitle_data)

            if output_path.exists():
                subtitle_paths.append(str(output_path))
                self.logger.info(f"Saved subtitle: {str(output_path)}")
            else:
                self.logger.info(f"Unable to write subtitles to disk, for: {query}")

        return subtitle_paths

    def _download_image(self, url: str, save_path: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            with open(save_path, 'wb') as f:
                f.write(response.content)

            self.logger.info(f"Downloaded artwork: {save_path}")
            return save_path

        except Exception as e:
            self.logger.error(f"Error downloading image {url}: {e}")
            return None
