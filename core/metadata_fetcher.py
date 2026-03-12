import logging
import os
import re
import time
from datetime import datetime
from functools import wraps
from typing import Dict, Any, Optional
import musicbrainzngs
import requests
from tmdbv3api import TMDb, Movie, TV, Search, Season, Episode
from core.TitleMatcher import TitleMatcher
from core.anilist_client import AniListClient
# Import our custom clients
from core.jikan_client import AnimeEpisodeFetcher
from core.music_metedata_fetcher import MusicMetadataExtractor
from core.omdb_client import OMDbClient
from core.tvdb_client import TVDBClient


# RETRY DECORATOR - ADD THIS RIGHT AFTER IMPORTS
def retry_api_call(max_retries=3, backoff_factor=1.0):
    """Decorator for retrying API calls with logging"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            logger = None

            # Try to get logger from self if available
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = backoff_factor * (2 ** attempt)
                        if logger:
                            logger.warning(
                                f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: "
                                f"{str(e)[:100]}... Retrying in {sleep_time:.1f}s"
                            )
                        time.sleep(sleep_time)
                    else:
                        if logger:
                            logger.error(
                                f"All {max_retries} attempts failed for {func.__name__}: {e}"
                            )
            raise last_exception

        return wrapper

    return decorator


class MetadataFetcher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.api_clients = {}  # Track which APIs are available
        self.titleMatcher = TitleMatcher()
        # Initialize API clients with better error handling
        self._initialize_api_clients()

    def _initialize_api_clients(self):
        """Initialize all API clients with proper error handling"""
        apis = {
            'tmdb': self._setup_tmdb,
            'tvdb': self._setup_tvdb,
            # 'musicbrainz': self._setup_musicbrainz,
            'anilist': self._setup_anilist,
            'jikan': self._setup_jikan,
            # 'omdb': self._setup_omdb,
            'music_extractor': self._setup_music_extractor
        }

        for api_name, setup_func in apis.items():
            try:
                success = setup_func()
                self.api_clients[api_name] = success
                self.logger.info(f"{api_name.upper()} API client initialized: {success}")
            except Exception as e:
                self.api_clients[api_name] = False
                self.logger.warning(f"Failed to initialize {api_name.upper()} API: {e}")

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_tmdb(self):
        """Setup TMDB API client"""
        try:
            self.tmdb = TMDb()
            self.tmdb.api_key = self.config['api_keys']['tmdb']
            self.tmdb.language = 'en'
            self.tmdb_movie = Movie()
            self.tmdb_tv = TV()
            self.tmdb_search = Search()
            self.tmdb_season = Season()
            self.tmdb_episode = Episode()
            return True
        except Exception as e:
            self.logger.error(f"Failed to setup TMDB: {e}")
            self.tmdb = None
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_omdb(self):
        """Setup TVDB client"""
        try:
            self.omdb_client = OMDbClient(self.config)
            test_result = self.omdb_client.search_movie_metadata("Guardians of the Galaxy Vol. 2", 2017)
            return test_result is not None
        except Exception as e:
            self.logger.error(f"Failed to setup OMDB client: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_music_extractor(self):
        """Setup music metadata extractor client"""
        try:
            self.music_extractor_client = MusicMetadataExtractor(self.config)
            return True
        except Exception as e:
            self.logger.error(f"Failed to setup music extractor client: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_tvdb(self):
        """Setup TVDB client"""
        try:
            self.tvdb_client = TVDBClient(self.config)
            # Test authentication
            return self.tvdb_client._authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup TVDB client: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_musicbrainz(self):
        """Setup MusicBrainz API client"""
        try:
            musicbrainzngs.set_useragent(
                self.config['api_keys']['musicbrainz'],
                "media-organizer/1.0",
                "https://github.com/yourusername/media-organizer"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to setup MusicBrainz: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_anilist(self):
        """Setup AniList client"""
        try:
            self.anilist_client = AniListClient(self.config)
            # Test with a simple search
            test_result = self.anilist_client.search_anime("Naruto")
            return test_result is not None
        except Exception as e:
            self.logger.error(f"Failed to setup AniList client: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _setup_jikan(self):
        """Setup jikan client"""
        try:
            self.jikan_client = AnimeEpisodeFetcher(self.config)
            # Test with a simple search
            test_result = self.jikan_client.search_anime("Naruto")
            return test_result is not None
        except Exception as e:
            self.logger.error(f"Failed to setup jikan client: {e}")
            return False

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def fetch_metadata(self, media_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch metadata based on media type with fallback strategies"""
        media_type = media_info['media_type']
        result = None
        try:
            # Get API priorities from config
            api_priorities = self.config.get('api_priorities', {}).get(media_type, ['filename'])

            for api_source in api_priorities:
                try:
                    if api_source == 'omdb' and media_type in ['movie']:
                        if media_type == 'movie':
                            result = self._fetch_movie_metadata3(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'tmdb' and media_type in ['movie', 'tv_show', 'anime']:
                        if media_type == 'movie':
                            result = self._fetch_movie_metadata2(media_info)
                        elif media_type in ['tv_show', 'anime']:
                            if not media_info.get('absolute_episode_number'):
                                result = self._fetch_tv_metadata(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'tvdb' and media_type in ['tv_show', 'anime']:
                        result = self._fetch_tvdb_metadata(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'jikan' and media_type == 'anime':
                        if media_info.get('absolute_episode_number'):
                            result = self._fetch_jikan_metadata2(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'anilist' and media_type == 'anime':
                        if media_info.get('absolute_episode_number'):
                            result = self._fetch_anilist_metadata(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'mutagen+spotify' and media_type == 'music':
                        result = self._fetch_music_metadata(media_info)
                        if result and result.get('source') != 'filename':
                            return result

                    elif api_source == 'special':
                        # Fallback to filename-based metadata
                        return self._get_filename_metadata(media_info)

                    elif api_source == 'unsorted':
                        # Fallback to filename-based metadata
                        return self._get_filename_metadata(media_info)

                    elif api_source == 'filename':
                        # Fallback to filename-based metadata
                        return self._get_filename_metadata(media_info)

                except Exception as e:
                    self.logger.warning(f"API {api_source} failed for {media_info.get('filename')}: {e}")
                    continue

            # Ultimate fallback
            return self._get_filename_metadata(media_info)

        except Exception as e:
            self.logger.error(f"Error fetching metadata for {media_info.get('filename')}: {e}")
            return self._get_filename_metadata(media_info)

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_movie_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch movie metadata from TMDB with fallback"""
        title = media_info.get('title')
        guessit_info = dict(media_info.get("guessit_info"))
        search_title = guessit_info.get("search_title")
        year = media_info.get('year')
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"
        data = []
        # Ensure title is a string, not bytes
        if isinstance(title, bytes):
            title = title.decode('utf-8', errors='ignore')
        title = str(title).strip()

        if hasattr(self, 'tmdb') and self.tmdb:
            try:
                search_query = str(title)
                while True:
                    # tmdbv3api Movie().search expects just the query string
                    results = self.tmdb_movie.search(search_query)

                    # Optional: if no results and year is available, try adding year into query
                    if (not results or not results.get("results")) and year:
                        results = self.tmdb_movie.search(f"{search_query} {year}")

                    if results and results.get("results"):
                        for result in results:
                            candidate_title = result.get("title") or result.get("original_title") or None
                            year = self.extract_movie_year(result) or None
                            data.append(
                                {
                                    "title": candidate_title,
                                    "year": year
                                }
                            )

                            best, scored = self.titleMatcher.match(search_query, data, content_type=media_type)

                            if best.get("score") >= 85:
                                movie_id = getattr(result, "id", None) or result.get("id")
                                if movie_id:
                                    details = self.tmdb_movie.details(movie_id)
                                    return self._format_movie_metadata(details)

                    if search_query != search_title:
                        search_query = str(search_title)
                    else:
                        break

            except Exception as e:
                self.logger.warning(f"TMDB movie search failed: {e}")

        # Fallback to manual TMDB API call
        try:
            return self._fetch_tmdb_manual(title, year)
        except Exception as e:
            self.logger.warning(f"Manual TMDB fetch failed: {e}")
            return self._get_filename_metadata(media_info)

    def _fetch_movie_metadata2(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch movie metadata from TMDB with fallback"""
        title = media_info.get('title')
        guessit_info = dict(media_info.get("guessit_info"))
        search_title = guessit_info.get("search_title")
        metadata_title = guessit_info.get("metadata_title")
        alt_title = guessit_info.get("alternative_title")
        year = media_info.get('year')
        original_filename = guessit_info.get("original_filename")
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"
        tmdb_id = media_info.get("tmdb_id")
        data = []

        if isinstance(title, bytes):
            title = title.decode('utf-8', errors='ignore')
        title = str(title).strip()

        if tmdb_id:
            details = self.tmdb_movie.details(tmdb_id)
            return self._format_movie_metadata(details)

        if hasattr(self, 'tmdb') and self.tmdb:
            try:
                for query in [title, alt_title, search_title, metadata_title]:
                    if not query:
                        continue

                    if year:
                        results = self.tmdb_search.movies(query, year=int(year))
                    else:
                        results = self.tmdb_movie.search(f"{query}")

                    # fallback logic
                    if not results or not results.get("results"):
                        if year:
                            results = self.tmdb_movie.search(f"{query} {int(year)}")
                        else:
                            results = self.tmdb_movie.search(f"{query}")

                    if results and results.get("results"):
                        for result in results:
                            candidate_title = result.get("title") or result.get("original_title") or None
                            candidate_year = self.extract_movie_year(result) or None

                            # Only append year if we have one, and it's not already in the title
                            normalized_title = query
                            if year and not re.search(rf"\b{year}\b", query):
                                normalized_title = f"{query} ({year})"

                            data = [
                                {
                                    "title": candidate_title,
                                    "year": candidate_year
                                }
                            ]

                            best, scored = self.titleMatcher.match(normalized_title, data, content_type=media_type)

                            if not best:
                                best = {
                                    "score": 0
                                }
                            if best.get("score") >= 90:
                                movie_id = result.get("id")
                                if movie_id:
                                    details = self.tmdb_movie.details(movie_id)
                                    return self._format_movie_metadata(details)

            except Exception as e:
                self.logger.warning(f"TMDB movie search failed: {e}")

        # Fallback to filename based metadata extraction
        try:
            return self._get_filename_metadata(media_info)
        except Exception as e:
            self.logger.warning(f"Manual TMDB fetch failed: {e}")
            return self._get_filename_metadata(media_info)

    def _fetch_movie_metadata3(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch movie metadata from OMDB with fallback"""
        title = media_info.get('title')
        guessit_info = dict(media_info.get("guessit_info"))
        search_title = guessit_info.get("search_title")
        year = media_info.get('year')

        if isinstance(title, bytes):
            title = title.decode('utf-8', errors='ignore')
        title = str(title).strip()

        if hasattr(self, 'tmdb') and self.tmdb:
            try:
                for query in {title, search_title}:
                    if not query:
                        continue

                    if year:
                        results = self.omdb_client.search_movie_metadata(query, int(year))
                    else:
                        results = self.omdb_client.search_movie_metadata(query)

                    if results and results.get("results"):
                        return results.get("results")

            except Exception as e:
                self.logger.warning(f"OMDB movie search failed: {e}")
                return self._get_filename_metadata(media_info)

    def _fetch_tmdb_manual(self, title: str, year: Optional[int] = None) -> Dict[str, Any]:
        """Manual TMDB API call as fallback"""
        import urllib.parse

        # Ensure proper URL encoding
        if isinstance(title, bytes):
            title = title.decode('utf-8', errors='ignore')
        title = str(title).strip()

        # URL encode the title
        encoded_title = urllib.parse.quote_plus(title)

        url = f"https://api.themoviedb.org/3/search/movie"
        params = {
            'api_key': self.config['api_keys']['tmdb'],
            'query': encoded_title,
            'language': 'en-US'
        }

        if year:
            params['year'] = year

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        if data.get('results'):
            movie_id = data['results'][0]['id']

            # Get movie details
            details_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
            details_response = self.session.get(details_url, params={
                'api_key': self.config['api_keys']['tmdb'],
                'language': 'en-US'
            })
            details_response.raise_for_status()

            return self._format_movie_metadata(details_response.json())

        raise ValueError("No results found in TMDB")

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_tv_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch TV show metadata from TMDB"""
        episode_data = {}
        data = []
        title = media_info.get('title', media_info['filename'])
        episode_title = media_info.get('episode_title', "Unknown Episode Title")
        season = int(media_info.get('season', 1))
        episode = int(media_info.get('episode', 1))
        guessit_info = dict(media_info.get("guessit_info"))
        original_filename = guessit_info.get("original_filename")
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"

        # Ensure series title is a proper string
        if isinstance(title, bytes):
            title = title.decode('utf-8', errors='ignore')
        title = str(title).strip()

        if not hasattr(self, 'tmdb') or not self.tmdb:
            return self._get_filename_metadata(media_info)

        try:
            # TMDB expects a clean string
            title = self.remove_year_from_title(title)
            search_query = title

            search_results = self.tmdb_search.tv_shows(search_query)
            if search_results and search_results['results']:
                # Pick the exact match based on title comparison (case-insensitive)
                exact_match = None
                for show in search_results['results']:
                    if show.get('name', '').lower() == title.lower():
                        exact_match = show
                        break

                    if not exact_match:
                        candidate_title = show.get('name', '').lower()
                        data = [
                            {
                                "title": candidate_title,
                                "season": season,
                                "episode": episode
                            }
                        ]

                    best, scored = self.titleMatcher.match(search_query, data, content_type=media_type)
                    if not best:
                        best = {
                            "score": 0
                        }
                    if best.get("score") > 90:
                        exact_match = show

                try:
                    if exact_match:
                        tv_id = int(exact_match.id)
                        episode_data = self.tmdb_episode.details(tv_id, season, episode)
                        if episode_data:
                            tmdb_episode_title = episode_data.get("name", "")
                            tmdb_episode = episode_data.get("episode_number", 0)
                            tmdb_season = episode_data.get("season_number", 0)
                            media_info_episode_title = media_info.get("episode_title", "")
                            if tmdb_episode_title and media_info_episode_title:
                                data = [
                                    {
                                        "title": tmdb_episode_title,
                                        "season": tmdb_season,
                                        "episode": tmdb_episode
                                    }
                                ]
                                best, scored = self.titleMatcher.match(media_info_episode_title, data,
                                                                       content_type=media_type)
                                if not best:
                                    best = {
                                        "score": 0
                                    }
                                if best.get("score") > 90:
                                    return {
                                        'series_id': exact_match.get('id'),
                                        'title': exact_match.get('name', title),
                                        'season': season,
                                        'episode': episode,
                                        'episode_title': episode_data.get("name"),
                                        'air_date': episode_data.get('air_date'),
                                        'overview': episode_data.get('overview'),
                                        'tvdb_id': int(exact_match.id),
                                        'source': 'tmdb'
                                    }
                                else:
                                    return self._get_filename_metadata(media_info)
                            else:
                                return {
                                    'series_id': exact_match.get('id'),
                                    'title': exact_match.get('name', title),
                                    'season': season,
                                    'episode': episode,
                                    'episode_title': episode_data.get("name"),
                                    'air_date': episode_data.get('air_date'),
                                    'overview': episode_data.get('overview'),
                                    'tvdb_id': int(exact_match.id),
                                    'source': 'tmdb'
                                }
                except Exception as e:
                    self.logger.warning(f"TMDB season/episode lookup failed: {e}")
        except Exception as e:
            self.logger.warning(f"TMDB TV search failed: {e}")

        return self._get_filename_metadata(media_info)

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_tvdb_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch metadata from TVDB"""
        title = media_info.get('title', media_info['filename'])
        season = media_info.get('season', 1)
        episode = media_info.get('episode', 1)
        absolute_episode_number = media_info.get('absolute_episode_number')
        tvdb_id = media_info.get("tvdb_id")
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"

        # Try TVDB if available
        if hasattr(self, 'tvdb_client') and self.api_clients.get('tvdb', False):
            try:
                if tvdb_id:
                    episode_data = self.tvdb_client.search_episode2(tvdb_id, season, episode)
                    return self._format_tv_metadata(episode_data)
                elif not absolute_episode_number:
                    episode_data = self.tvdb_client.search_episode(title, season, episode)
                else:
                    episode_data = self.tvdb_client.search_episode_by_episode_number(title, absolute_episode_number)

                if not episode_data and not absolute_episode_number:
                    episode_data = self.tvdb_client.search_episode_by_episode_number(title, episode)

                if episode_data:
                    tvdb_episode_title = episode_data.get("episode_title", "")
                    tvdb_episode = episode_data.get("episode", 0)
                    tvdb_season = episode_data.get("season", 0)
                    media_info_episode_title = media_info.get("episode_title", "")
                    if tvdb_episode_title and media_info_episode_title:
                        data = [
                            {
                                "title": tvdb_episode_title,
                                "season": tvdb_season,
                                "episode": tvdb_episode
                            }
                        ]
                        best, scored = self.titleMatcher.match(media_info_episode_title, data, content_type=media_type)
                        if not best:
                            best = {
                                "score": 0
                            }
                        if best.get("score") > 90:
                            return self._format_tv_metadata(episode_data)
                        else:
                            return self._get_filename_metadata(media_info)
                    else:
                        return self._format_tv_metadata(episode_data)
            except Exception as e:
                self.logger.warning(f"TVDB episode search failed: {e}")

        return self._get_filename_metadata(media_info)

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_anilist_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch anime metadata from AniList"""
        title = media_info.get('title', media_info['filename'])
        episode = media_info.get('absolute_episode_number', 1)
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"
        formatted_episode_data = {}

        # Try AniList if available
        if hasattr(self, 'anilist_client') and self.api_clients.get('anilist', False):
            try:
                anime_data = self.anilist_client.search_anime(title)
                if anime_data:
                    episode_data = self.anilist_client.get_episode_details_by_number(int(anime_data.get("id")), episode)

                    if episode_data:
                        formatted_episode_data = self.anilist_client.format_episode_metadata(anime_data, episode_data)
                        clean_episode_title = self.anilist_client.clean_episode_title(
                            formatted_episode_data.get("episode_title"))
                        if clean_episode_title and media_info.get("episode_title"):
                            data = [
                                {
                                    "title": clean_episode_title,
                                    "season": int(formatted_episode_data.get("season")),
                                    "episode": int(formatted_episode_data.get("episode"))
                                }
                            ]
                            best, scored = self.titleMatcher.match(media_info.get("episode_title"), data,
                                                                   content_type=media_type)
                            if not best:
                                best = {
                                    "score": 0
                                }
                            if best.get("score") > 90:
                                return formatted_episode_data
                            else:
                                return self._get_filename_metadata(media_info)

                        else:
                            return formatted_episode_data
            except Exception as e:
                self.logger.warning(f"AniList search failed: {e}")

        return self._get_filename_metadata(media_info)

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_jikan_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch anime metadata from MyAnimeList"""
        anime_title = media_info.get('title', media_info['filename'])
        episode_number = media_info.get('episode', 1)

        # Try MyAnimeList if available
        if hasattr(self, 'jikan_client') and self.api_clients.get('jikan', False):
            try:
                anime_data = self.jikan_client.search_anime(anime_title)
                if anime_data:
                    return self.jikan_client.get_episode_details(anime_title, episode_number, anime_data)
            except Exception as e:
                self.logger.warning(f"AniList search failed: {e}")

        return self._get_filename_metadata(media_info)

    @retry_api_call(max_retries=3, backoff_factor=1.0)
    def _fetch_jikan_metadata2(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch anime metadata from MyAnimeList"""
        anime_title = media_info.get("title", media_info["filename"])
        episode_number = media_info.get("episode", 1)
        media_type = "movie" if media_info.get('media_type') == "movie" else "series"

        if hasattr(self, "jikan_client") and self.api_clients.get("jikan", False):
            try:
                anime_data = self.jikan_client.search_anime2(anime_title)
                if anime_data:
                    episode_deta = self.jikan_client.get_episode_details2(
                        anime_title,
                        episode_number,
                        anime_data,
                    )
                    if episode_deta:
                        if episode_deta.get("episode_title") and media_info.get("episode_title"):
                            data = [
                                {
                                    "title": episode_deta.get("episode_title"),
                                    "season": int(episode_deta.get("season")),
                                    "episode": int(episode_deta.get("episode"))
                                }
                            ]
                            best, scored = self.titleMatcher.match(media_info.get("episode_title"), data,
                                                                   content_type=media_type)
                            if not best:
                                best = {
                                    "score": 0
                                }
                            if best.get("score") > 90:
                                return episode_deta
                            else:
                                return self._get_filename_metadata(media_info)

                        else:
                            return episode_deta
            except Exception as e:
                self.logger.warning(f"Jikan search failed: {e}")

        return self._get_filename_metadata(media_info)

    def _fetch_music_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract music metadata using MusicMetadataExtractor.
        Fallback order:
          1. File tags via Mutagen
          2. Filename parsing
          3. Spotify search (including anime-boosted search)
        """
        filename = media_info.get("filename")
        guessit_info = dict(media_info.get("guessit_info", {}))
        filepath = guessit_info.get("original_file_path")

        # Parent folder often contains album name
        parent_folder = os.path.basename(os.path.dirname(filepath))

        # Initialize extractor
        extractor = MusicMetadataExtractor(getattr(self, "config", {}))

        # Extract metadata
        metadata = extractor.extract_metadata(media_info)

        # Ensure search_title & original_filename are preserved
        search_title = metadata.get("title") or guessit_info.get("search_title")
        original_filename = guessit_info.get("original_filename", filename)

        return {
            "artist": metadata.get("artist"),
            "album": metadata.get("album"),
            "title": metadata.get("title"),
            "track": metadata.get("track"),
            "year": metadata.get("year"),
            "cover_url": metadata.get("cover_url"),
            "search_title": search_title,
            "original_filename": original_filename,
            "filepath": filepath,
            "source": metadata.get("source"),
        }

    def _get_filename_metadata(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from filename as fallback"""
        title = str(media_info["title"]).strip()
        media_type = media_info['media_type']
        guessit_info = dict(media_info.get("guessit_info"))
        search_title = guessit_info.get("search_title")
        metadata_title = guessit_info.get("metadata_title")
        original_filename = guessit_info.get("original_filename")
        episode_title = media_info.get("episode_title")
        version = guessit_info.get("version")

        if episode_title:
            episode_title = f'{episode_title}'
        else:
            episode_title = f'Episode {media_info.get("episode", 0)}'

        if version:
            if episode_title:
                episode_title = f'{episode_title} V{version}'
            else:
                episode_title = f'Episode {media_info.get("episode", 0)}-V{version}'

        filename_only = os.path.splitext(original_filename)[0]

        if media_type == 'movie':
            return {
                'title': title or search_title or filename_only,
                'year': media_info.get('year'),
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }
        elif media_type in ['tv_show', 'anime']:
            return {
                'title': title or search_title or filename_only,
                'season': media_info.get('season', 0),
                'episode': media_info.get('episode', 0),
                'episode_title': episode_title,
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }
        elif media_type in ['special']:
            return {
                'title': title or search_title or filename_only,
                'season': media_info.get('season', 0),
                'episode': media_info.get('episode', 0),
                'episode_title': episode_title,
                'anime_special_type': media_info.get('anime_special_type'),
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }
        elif media_type in ['unsorted']:
            return {
                'title': title or search_title or filename_only,
                'year': media_info.get('year'),
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }
        elif media_type == 'music':
            return {
                'artist': media_info.get('artist', 'Unknown Artist'),
                'album': media_info.get('album', 'Unknown Album'),
                'title': title or search_title or filename_only,
                'track': media_info.get('track', 0),
                'year': media_info.get('year'),
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }
        else:
            return {
                'title': title or search_title or filename_only,
                'search_title': search_title,
                'original_filename': original_filename,
                'source': 'filename'
            }

    def _format_movie_metadata(self, tmdb_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format TMDB movie data into our standard format"""
        return {
            'title': tmdb_data.get('title'),
            'original_title': tmdb_data.get('original_title'),
            'year': tmdb_data.get('release_date', '')[:4] if tmdb_data.get('release_date') else None,
            'overview': tmdb_data.get('overview'),
            'genres': [genre['name'] for genre in tmdb_data.get('genres', [])],
            'rating': tmdb_data.get('vote_average'),
            'runtime': tmdb_data.get('runtime'),
            'poster_path': tmdb_data.get('poster_path'),
            'backdrop_path': tmdb_data.get('backdrop_path'),
            'tmdb_id': tmdb_data.get("id"),
            'imdb_id': tmdb_data.get('imdb_id'),
            'source': 'tmdb'
        }

    def _format_tv_metadata(self, tvdb_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format TVDB series/anime data into our standard format"""
        return {
            'title': tvdb_data.get('title'),
            'season': tvdb_data.get('season'),
            'episode': tvdb_data.get('episode'),
            'episode_title': tvdb_data.get('episode_title'),
            'overview': tvdb_data.get('overview'),
            'year': tvdb_data.get('year'),
            'tvdb_id': tvdb_data.get("tvdb_id"),
            'image': tvdb_data.get("image"),
            'source': 'tvdb'
        }

    def get_api_status(self) -> Dict[str, bool]:
        """Get status of all API clients"""
        return self.api_clients.copy()

    def extract_movie_year(self, movie_data: dict) -> int | None:
        """
        Extracts a movie's year from TMDB result.
        Prioritizes 'year' key, falls back to 'release_date'.
        Returns None if neither is valid.
        """
        # 1. Direct year field
        if "year" in movie_data and movie_data["year"]:
            try:
                return int(movie_data["year"])
            except ValueError:
                pass

        # 2. Fallback to release_date (e.g., '1986-07-18')
        release_date = movie_data.get("release_date")
        if release_date and len(release_date) >= 4:
            try:
                return datetime.strptime(release_date, "%Y-%m-%d").year
            except ValueError:
                # handle partial or invalid dates (e.g., '1986')
                if release_date[:4].isdigit():
                    return int(release_date[:4])

        # 3. Nothing found
        return None

    def remove_year_from_title(self, title: str) -> str:
        # Removes patterns like "(2023)" or "(1999)"
        return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
