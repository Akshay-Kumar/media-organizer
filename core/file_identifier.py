import datetime as dt
import logging
import mimetypes
import os
import re
from collections.abc import Iterator
from datetime import datetime
from os.path import splitdrive, splitext
from pathlib import Path
from typing import Dict, Any, Optional
from guessit import guessit
from rapidfuzz import fuzz
from unicodedata import normalize

import utils.torrent_metadata
from core.TitleMatcher import TitleMatcher
from core.language import Language
from core.media_parser import MediaParser
from core.metadata_fetcher import MetadataFetcher
from utils import torrent_metadata
from utils.anime_keywords import get_anime_keywords
from utils.media_extensions import get_media_extensions
from utils.name_cleaner import clean_media_name, validate_episode_title, validate_season_and_episode_number, \
    sanitize_guess_data
from utils.special_media_detection import parse_path


class MediaFileIdentifier:
    def __init__(self, config: Dict[str, Any], metadata_fetcher: MetadataFetcher, media_parser: MediaParser):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.titleMatcher = TitleMatcher()
        self.metadata_fetcher = metadata_fetcher
        self.media_matcher = media_parser
        self.get_media_extensions = get_media_extensions()
        self.get_anime_keywords = get_anime_keywords()
        # Initialize mimetypes database
        mimetypes.init()

    def is_media_file(self, file_path: Path) -> bool:
        """Check if file is a media file using extension-based approach"""
        if not file_path.is_file():
            return False

        # Use extension-based detection
        media_extensions = self.get_media_extensions
        return file_path.suffix.lower() in media_extensions["all"]

    def get_series_regex(self, regex_type: str = 'all'):
        # Special prefixes for TV content (OP/ED/OVA/Specials)
        SPECIAL_PREFIXES = [
            r'^(?P<show>.*?) - (OP|ED|OVA|Special)(?P<num>[0-9]{1,2})[ _\-]+(?P<title>.*)$',
            r'^(?P<show>.*?) \[(OP|ED|OVA|Special)(?P<num>[0-9]{1,2})\] (?P<title>.*)$',
            r'^(?P<show>.*?) \((OP|ED|OVA|Special)(?P<num>[0-9]{1,2})\) (?P<title>.*)$'
        ]

        # Example SERIES_RX (can extend with your full regex list)
        SERIES_RX = [
            r'(^|(?P<show>.*?)[ _\.\-]+)s(?P<season>[0-9]{1,2})(e| e|ep| ep|-)(?P<ep>[0-9]{1,3})(([ _\.\-]|(e|ep)|[ '
            r'_\.\-](e|ep))(?P<ep2>[0-9]{1,3}))?($|( | - |)(?P<title>.*?)$)',
            r'^(?P<show>.*?) - (E|e|Ep|ep|EP)?(?P<ep>[0-9]{1,3})(-(?P<ep2>[0-9]{1,3}))?( - )?(?P<title>.*)$'
        ]
        if regex_type == 'SPECIAL_PREFIXES':
            return SPECIAL_PREFIXES
        elif regex_type == 'SERIES_RX':
            return SERIES_RX
        else:
            return [*SERIES_RX, *SPECIAL_PREFIXES]

    def enrich_with_torrent_metadata(self, t_metadata: dict, guess: dict):
        # Step: Enrich + Override file metadata using torrent metadata
        if t_metadata and isinstance(t_metadata, dict):
            t_metadata = dict(t_metadata)
            tmdb_id = t_metadata.get("tmdb_id")
            guess["tmdb_id"] = tmdb_id
            logging.info(f"Organizerr: tmdb_id: {tmdb_id}")

            year = t_metadata.get("year")
            logging.info(f"Organizerr: year: {year}")

            title = t_metadata.get("correct_name") or t_metadata.get("name")
            logging.info(f"Organizerr: title: {title}")

            season = t_metadata.get("season")
            logging.info(f"Organizerr: season: {season}")

            episode = t_metadata.get("episode")
            logging.info(f"Organizerr: episode: {episode}")

            episode_title = t_metadata.get("episode_title")
            logging.info(f"Organizerr: episode_title: {episode_title}")

            media_type = "episode" if t_metadata.get("media_type") == "tv" else t_metadata.get("media_type")
            logging.info(f"Organizerr: media_type: {media_type}")

            guess_media_type = guess.get("type")
            guess_title = guess.get("title")
            guess_year = guess.get("year")
            guess_episode_title = guess.get("episode_title")
            guess_episode = guess.get("episode")
            guess_season = guess.get("season")

            if media_type and media_type != guess_media_type:
                logging.info(f"Organizerr: updating media_type from '{guess_media_type}' to '{media_type}'")
                guess["type"] = media_type

            if title and title != guess_title:
                logging.info(f"Organizerr: updating title from '{guess_title}' to '{title}'")
                guess["title"] = title

            if year and year != guess_year:
                logging.info(f"Organizerr: updating year from '{guess_year}' to '{year}'")
                guess["year"] = year

            if media_type == "episode":
                if season and guess_season != season:
                    logging.info(f"Organizerr: updating season from '{guess_season}' to '{season}'")
                    guess["season"] = int(season)

                if episode and episode != guess_episode:
                    logging.info(f"Organizerr: updating episode from '{guess_episode}' to '{episode}'")
                    guess["episode"] = int(episode)

                if episode_title and episode_title != guess_episode_title:
                    logging.info(
                        f"Organizerr: updating episode_title from '{guess_episode_title}' to '{episode_title}'")
                    guess["episode_title"] = episode_title
            elif media_type == "unsorted":
                if guess.get("season"):
                    guess.pop("season")

                if guess.get("episode"):
                    guess.pop("episode")

                if guess.get("episode_title"):
                    guess.pop("episode_title")

        return guess

    def identify(self, file_path: Path, info_hash: str = None) -> Dict[str, Any]:
        """Identify media type and extract normalized information using GuessIt + custom logic"""
        filename = file_path.name
        result = {
            "filename": filename,
            "file_path": str(file_path),
            "file_extension": file_path.suffix.lower(),
            "extension": file_path.suffix.lower(),
            "media_type": None,
            "guessit_info": {},
        }

        try:
            # Pre Checks: Detect special media types
            guess = parse_path(str(file_path))

            # Step 1: Parse + normalize metadata
            guess = self.parse_filename(guess, file_path, info_hash=info_hash)

            # Step 2: Enhance episodes with smart guessing and parent folder info
            if guess.get("type") == "episode":
                guess = self._enhance_with_parent_info(guess, file_path)

            # Step 3: Decide best title
            best_title = self._pick_best_title(guess, file_path)
            if guess.get("type") in ("episode", "movie"):
                guess["title"] = best_title

            # Step 4: Assign media_type + run normalization + extract info
            if guess.get("type") == "episode":
                if self._is_anime(guess):
                    guess = self._normalize_anime_info(guess)
                    result["media_type"] = "anime"
                    result.update(self._extract_anime_info(guess))
                else:
                    guess = self._normalize_tv_info(guess)
                    result["media_type"] = "tv_show"
                    result.update(self._extract_tv_info(guess))

            elif guess.get("type") == "movie":
                guess = self._normalize_movie_info(guess)
                result["media_type"] = "movie"
                result.update(self._extract_movie_info(guess))

            elif guess.get("type") == "music":
                guess = self._normalize_music_info(guess)
                result["media_type"] = "music"
                result.update(self._extract_music_info(guess))

            elif guess.get("type") == "special":
                guess = self._normalize_special_tv_info(guess)
                result["media_type"] = "special"
                result.update(self._extract_special_tv_info(guess))

            elif guess.get("type") == "unsorted":
                guess = self._normalize_unsorted_info(guess)
                result["media_type"] = "unsorted"
                result.update(self._extract_unsorted_info(guess))
            else:
                # Fallback to extension-based detection
                result["media_type"] = self._fallback_identification(file_path)
                result.update(
                    {
                        "title": filename,
                        "season": None,
                        "episode": None,
                        "episode_title": None,
                        "media_type": None,
                        "year": None,
                    }
                )

            # Step 5: Save parsed info for debugging/introspection
            result["guessit_info"] = guess

        except Exception as e:
            self.logger.error(f"Error identifying file {filename}: {e}")
            result["media_type"] = self._fallback_identification(file_path)

        return result

    def get_extension(self, filename: str) -> str:
        """Return lowercase file extension (including dot), or empty string if none."""
        return os.path.splitext(filename)[1].lower()

    def _sanitize_text(self, text: Optional[str]) -> str:
        """
        Normalize and sanitize text fields (title, artist, series, etc.)
        - Replaces underscores/dots with spaces
        - Collapses multiple spaces
        - Strips leading/trailing spaces
        """
        if not text:
            return ""
        cleaned = str(text)
        cleaned = cleaned.replace("_", " ").replace(".", " ")
        cleaned = re.sub(r"\s+", " ", cleaned)  # collapse multiple spaces
        return cleaned.strip()

    def detect_episode_format(self, filename: str):
        season_episode_patterns = [
            re.compile(r'[Ss](\d{1,2})[Ee](\d{1,2})'),  # S01E05
            re.compile(r'(\d{1,2})[xX](\d{1,2})'),  # 1x05
        ]
        absolute_episode_pattern = re.compile(r'(?<!\d)(\d{2,4})(?!\d)')

        # Check SxxExx or 1x01
        for pattern in season_episode_patterns:
            if pattern.search(filename):
                return "season_episode"

        # Check absolute numbering
        if absolute_episode_pattern.search(filename):
            return "absolute"

        return "unknown"

    def findall(self, s, ss) -> Iterator[int]:
        """Yields indexes of all start positions of substring matches in string."""
        i = s.find(ss)
        while i != -1:
            yield i
            i = s.find(ss, i + 1)

    def is_subtitle(self, container: str | Path | None) -> bool:
        """Returns True if container is a subtitle container."""
        SUBTITLE_CONTAINERS = [".srt", ".idx", ".sub"]
        if not container:
            return False
        return str(container).endswith(tuple(SUBTITLE_CONTAINERS))

    def str_sanitize(self, filename: str) -> str:
        """Removes illegal filename characters and condenses whitespace."""

        if not filename:
            return filename

        base, container = splitext(filename)
        if self.is_subtitle(container):
            base = base.rstrip(".")
            base, container_prefix = splitext(base)
            container = container_prefix + container
        base = re.sub(r"\s+", " ", base)
        drive, tail = splitdrive(base)
        tail = re.sub(r'[<>:"|?*&%=+@#`^]', "", tail)
        return drive + tail.strip("-., ") + container

    def str_title_case(self, s: str) -> str:
        """Attempts to intelligently apply title case transformations to strings."""

        if not s:
            return s

        lowercase_exceptions = {
            "a",
            "an",
            "and",
            "as",
            "at",
            "but",
            "by",
            "de",
            "des",
            "du",
            "for",
            "from",
            "in",
            "is",
            "le",
            "nor",
            "of",
            "on",
            "or",
            "the",
            "to",
            "un",
            "une",
            "with",
            "via",
        }
        uppercase_exceptions = {
            "i",
            "ii",
            "iii",
            "iv",
            "v",
            "vi",
            "vii",
            "viii",
            "ix",
            "x",
            "2d",
            "3d",
            "au",
            "aka",
            "atm",
            "bbc",
            "bff",
            "cia",
            "csi",
            "dc",
            "doa",
            "espn",
            "fbi",
            "ira",
            "jfk",
            "lol",
            "mlb",
            "mlk",
            "mtv",
            "nba",
            "nfl",
            "nhl",
            "nsfw",
            "nyc",
            "omg",
            "pga",
            "oj",
            "rsvp",
            "tnt",
            "tv",
            "ufc",
            "ufo",
            "uk",
            "usa",
            "vip",
            "wtf",
            "wwe",
            "wwi",
            "wwii",
            "xxx",
            "yolo",
        }
        padding_chars = ".- "
        paren_chars = "[](){}<>{}"
        punctuation_chars = paren_chars + "\"!?$,-.:;@_`'"
        partition_chars: str = padding_chars + punctuation_chars
        string_lower = s.lower()
        string_length = len(s)

        # uppercase first character
        s = s.lower()
        s = s[0].upper() + s[1:]

        # uppercase characters after padding characters
        for char in padding_chars:
            for pos in self.findall(s, char):
                if pos + 1 == string_length:
                    break
                elif pos + 2 < string_length:
                    s = s[: pos + 1] + s[pos + 1].upper() + s[pos + 2:]
                else:
                    s = s[: pos + 1] + s[pos + 1].upper()

        # uppercase characters inside parentheses
        for char in paren_chars:
            for pos in self.findall(s, char):
                if pos > 0 and s[pos - 1] not in padding_chars:
                    continue
                elif pos + 1 < string_length:
                    s = s[: pos + 1] + s[pos + 1].upper() + s[pos + 2:]

        # process lowercase transformations
        for exception in lowercase_exceptions:
            for pos in self.findall(string_lower, exception):
                is_start = pos < 2
                if is_start:
                    break
                prev_char = string_lower[pos - 1]
                is_left_partitioned = prev_char in padding_chars
                word_length = len(exception)
                ends = pos + word_length == string_length
                next_char = "" if ends else string_lower[pos + word_length]
                is_right_partitioned = not ends and next_char in padding_chars
                if is_left_partitioned and is_right_partitioned:
                    s = s[:pos] + exception.lower() + s[pos + word_length:]

        # process uppercase transformations
        for exception in uppercase_exceptions:
            for pos in self.findall(string_lower, exception):
                is_start = pos == 0
                prev_char = None if is_start else string_lower[pos - 1]  # type: ignore
                is_left_partitioned = is_start or prev_char in partition_chars  # type: ignore
                word_length = len(exception)
                ends = pos + word_length == string_length
                next_char = "" if ends else string_lower[pos + word_length]
                is_right_partitioned = ends or next_char in partition_chars
                if is_left_partitioned and is_right_partitioned:
                    s = s[:pos] + exception.upper() + s[pos + word_length:]

        return s

    def str_scenify(self, filename: str) -> str:
        """Replaces non ascii-alphanumerics with dots."""
        filename = normalize("NFKD", filename)
        filename.encode("ascii", "ignore")
        filename = re.sub(r"\s+", ".", filename)
        filename = re.sub(r"[^.\d\w/]", "", filename)
        filename = re.sub(r"\.+", ".", filename)
        return filename.lower().strip(".")

    def str_scenify2(self, filename: str) -> str:
        """Replaces non-ascii-alphanumerics with space and removes junk characters."""
        # Normalize Unicode
        filename = normalize("NFKD", filename)

        # Remove non-ASCII characters
        filename = filename.encode("ascii", "ignore").decode("ascii")

        # Replace whitespace with dots
        filename = re.sub(r"\s+", ".", filename)

        # Remove unwanted characters except alnum, dot, slash
        filename = re.sub(r"[^.\d\w/]", "", filename)

        # Collapse multiple dots
        filename = re.sub(r"\.+", ".", filename)

        return filename.lower().strip(".")

    def str_fix_padding(self, s: str) -> str:
        """Truncates and collapses whitespace and delimiters in strings."""
        len_before = len(s)
        # Remove empty brackets
        s = re.sub(r"\(\s*\)", "", s)
        s = re.sub(r"\[\s*]", "", s)
        # Collapse dashes
        s = re.sub(r"-+", "-", s)
        # Collapse whitespace
        s = re.sub(r"\s+", " ", s)
        # Collapse repeating delimiters
        s = re.sub(r"( [-.,_])+", r"\1", s)
        # Strip leading/ trailing whitespace
        s = s.strip()
        # Strip leading/ trailing dashes
        s = s.strip("-")
        len_after = len(s)
        return s if len_before == len_after else self.str_fix_padding(s)

    def normalize_text(self, s: str) -> str:
        """Lowercase, collapse whitespace/punctuation for comparison."""
        if not s:
            return ""
        s = s.lower()
        s = re.sub(r"[^a-z0-9]+", " ", s)  # keep alnum, replace others
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def fix_media_title(self, title: str) -> str:
        """
        Replace hyphens in a media title with spaces, while keeping proper words intact.
        Example: 'my-hero-acedemia' -> 'My Hero Acedemia'
        """
        if not title:
            return title

        # Replace hyphens surrounded by letters/numbers with space
        title = re.sub(r'(?<=\w)-(?=\w)', ' ', title)

        # Remove multiple spaces
        title = re.sub(r'\s+', ' ', title).strip()

        # Capitalize each word (optional)
        title = self.str_title_case(title)

        return title

    def enrich_metadata(self, raw_data: Dict[str, Any], parsed_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[
        str, Any]:
        raw_title = self.normalize_text(raw_data.get("title"))
        raw_ep_title = self.normalize_text(raw_data.get("episode_title"))
        alt_title = raw_data.get("alternative_title")
        if isinstance(alt_title, list):
            alt_title = " ".join(alt_title)
        alternative_title = self.normalize_text(alt_title)
        search_title = self.normalize_text(parsed_data.get("search_title"))

        # Fuzzy scores
        title_score = fuzz.partial_ratio(raw_title, search_title) if raw_title else 0
        ep_title_score = fuzz.partial_ratio(raw_ep_title, search_title) if raw_ep_title else 0
        alt_title_score = fuzz.partial_ratio(alternative_title, search_title) if alternative_title else 0

        if raw_data.get("type") == "movie":
            if max(title_score, ep_title_score, alt_title_score) < 80:  # relax threshold a bit
                new_guess = dict(guessit(str(parsed_data.get("filename")), options))
                # Merge instead of replace
                raw_data.update(new_guess)

            raw_data["season"] = None
            raw_data["episode"] = None
            raw_data["series"] = None
            raw_data["year"] = parsed_data.get("year")

        # Keep alternative title separate
        if alternative_title:
            raw_data["alternative_title"] = alternative_title

        # Always restore search_title for downstream API calls
        raw_data["search_title"] = parsed_data.get("search_title")

        return raw_data

    def enrich_metadata2(self, raw_data: Dict[str, Any], parsed_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[
        str, Any]:
        data = []
        expected_title = []
        new_guess = {}
        if not raw_data.get("title"):
            return raw_data

        title = raw_data.get("title")
        alt_title = raw_data.get("alternative_title")
        if isinstance(alt_title, list):
            alt_title = " ".join(alt_title)
        alternative_title = self.normalize_text(alt_title)
        episode_title = self.normalize_text(raw_data.get("episode_title"))
        search_title = self.normalize_text(parsed_data.get("search_title"))
        metadata_title = self.normalize_text(parsed_data.get("metadata_title"))
        file_path = raw_data.get("file_path")
        parent_directory = file_path.parent.name
        search_title = self.str_title_case(search_title)
        metadata_title = self.str_title_case(metadata_title)
        content_type = None

        if raw_data.get("type") == "movie":
            content_type = "movie"
            data = [
                {
                    "title": title,
                    "year": raw_data.get("year", None)
                }
            ]
        elif raw_data.get("type") == "episode":
            content_type = "series"
            data = [
                {
                    "title": title,
                    "season": raw_data.get("season", None),
                    "episode": raw_data.get("episode", None)
                }
            ]

        # filename match
        search_best, search_scored = self.titleMatcher.match(search_title, data, content_type=content_type)

        # filename match
        metadata_best, metadata_scored = self.titleMatcher.match(metadata_title, data, content_type=content_type)

        # directory name match
        dir_best, dir_scored = self.titleMatcher.match(parent_directory, data, content_type=content_type)

        if not search_best:
            search_best = {
                "score": 0
            }

        if not metadata_best:
            metadata_best = {
                "score": 0
            }

        if not dir_best:
            dir_best = {
                "score": 0
            }

        if dir_best.get("score") >= 95:
            search_best = {
                "score": 100
            }

            metadata_best = {
                "score": 100
            }

        if search_best.get("score") <= 90 and metadata_best.get("score") <= 90:
            expected_title.append(search_title)
            options.update({"expected_title": expected_title})
            new_guess = dict(guessit(str(parsed_data.get("filename")), options))
            # Merge instead of replace
            raw_data.update(new_guess)

            if raw_data.get("type") == "movie" and new_guess:
                for key in (
                        "season", "episode", "episode_title", "episodes", "episode_count", "season_count", "series"):
                    if key in raw_data:
                        raw_data.pop(key)

                if str(raw_data.get("title")).strip().lower() == str(title).strip().lower() \
                        and title not in search_title:
                    raw_data["title"] = search_title

            if raw_data.get("type") == "episode" and new_guess:
                for key in ("film", "movie"):
                    if key in raw_data:
                        raw_data.pop(key)

        # Keep alternative title separate
        if alternative_title:
            raw_data["alternative_title"] = alternative_title

        # Always restore search_title for downstream API calls
        raw_data["search_title"] = search_title
        raw_data["metadata_title"] = metadata_title

        return raw_data

    def compare_titles(self, value1: str, value2: str) -> float:
        score = self.media_matcher.compare_titles(value1, value2)
        if not score:
            score = 0.0
        return score

    def compare_series_titles(self, title: str, tvdb_title: str) -> float:
        data = [
            {
                "title": tvdb_title,
                "season": None,
                "episode": None
            }
        ]
        search_best, search_scored = self.titleMatcher.match(title, data, content_type="series")  # filename match
        if not search_best:
            search_best = {
                "score": 0
            }
        return float(search_best.get("score"))

    def validate_release_year(self, file_year: int, db_year: int, score: float) -> int:
        # if title or alias or slug matches, also match year if provided
        final_score = 0.0
        if file_year and db_year:
            year_diff = abs(file_year - db_year)
            if year_diff == 0:
                final_score = max(score, 100)
            else:
                if 3 > year_diff > 0:
                    final_score = (score - 20)
                else:
                    final_score = 0
        else:
            final_score = score

        return final_score

    def validate_series_name(self, title: str, year: int = None) -> tuple[float, str | None, str | None]:
        """Check if a title exists in TVDB and return corrected name if found."""
        threshold_score = 90.0
        final_score, final_title = 0.0, None
        final_tvdb_id = None
        try:
            if not title:
                return 0.0, None, None

            # tvdb_results = self.metadata_fetcher.tvdb_client.search_series(title)
            # tvdb_results = self.metadata_fetcher.tvdb_client.tvdb_v4_official.search(query=title)

            # test to add year in series search
            if year:
                tvdb_results = self.metadata_fetcher.tvdb_client.tvdb_v4_official.search(f"{title} ({year})")
            else:
                tvdb_results = self.metadata_fetcher.tvdb_client.tvdb_v4_official.search(f"{title}")

            if tvdb_results:
                if isinstance(tvdb_results, list):
                    if len(tvdb_results) > 0:
                        for result in tvdb_results:
                            tvdb_title = str(result.get("name")) if result.get("name") else None
                            tvdb_year = int(result.get("year")) if result.get("year") else None
                            tvdb_slug = str(result.get("slug")) if result.get("slug") else None
                            tvdb_aliases = list(result.get("aliases")) if result.get("aliases") else None
                            tvdb_translations = dict(result.get("translations")) if result.get("translations") else None
                            tvdb_translations_eng = tvdb_translations.get("eng") or None
                            tvdb_id = result.get("tvdb_id")

                            # title check
                            if tvdb_title:
                                title_score = self.compare_series_titles(title, tvdb_title)
                                if title_score >= threshold_score:
                                    if tvdb_year and year:
                                        title_score = self.validate_release_year(file_year=year,
                                                                                 db_year=tvdb_year,
                                                                                 score=title_score)
                                    if title_score >= threshold_score:
                                        final_title = tvdb_translations_eng or tvdb_title
                                        final_score = title_score
                                        final_tvdb_id = tvdb_id
                                        break

                            # slug check
                            if tvdb_slug:
                                slug_score = self.compare_series_titles(title, tvdb_slug)
                                if slug_score >= threshold_score:
                                    if tvdb_year and year:
                                        slug_score = self.validate_release_year(file_year=year,
                                                                                db_year=tvdb_year,
                                                                                score=slug_score)
                                    if slug_score >= threshold_score:
                                        final_title = tvdb_translations_eng or tvdb_slug
                                        final_score = slug_score
                                        final_tvdb_id = tvdb_id
                                        break

                            # alias check
                            if tvdb_aliases:
                                if isinstance(tvdb_aliases, list):
                                    for alias in tvdb_aliases:
                                        alias_score = self.compare_series_titles(title, alias)
                                        if alias_score >= threshold_score:
                                            if tvdb_year and year:
                                                alias_score = self.validate_release_year(file_year=year,
                                                                                         db_year=tvdb_year,
                                                                                         score=alias_score)
                                            if alias_score >= threshold_score:
                                                final_title = tvdb_translations_eng or alias
                                                final_score = alias_score
                                                final_tvdb_id = tvdb_id
                                                break
                                    return final_score, final_title, final_tvdb_id

                return final_score, final_title, final_tvdb_id
        except Exception as e:
            self.logger.debug(f"TVDB validation failed for {title}: {e}")
        return 0.0, None, None

    def validate_movie_name(self, title: str, year: int = None) -> tuple[float, str | None, str | None]:
        """Check if a movie title exists in TMDB and return corrected name if found."""
        threshold_score = 90.0
        final_score, final_title = 0.0, None
        final_tmdb_id = None
        try:
            if not title:
                return 0.0, None, None

            if year:
                tmdb_results = self.metadata_fetcher.tmdb_search.movies(title, year=int(year))
            else:
                tmdb_results = self.metadata_fetcher.tmdb_movie.search(f"{title}")

            if tmdb_results and tmdb_results.get("results"):
                results = list(tmdb_results.get("results")) if isinstance(tmdb_results.get("results"), list) \
                    else tmdb_results.get("results")
                if len(results) > 0:
                    for result in results:
                        tmdb_title = str(result.get("title")) if result.get("title") else None
                        tmdb_id = result.get("id")

                        # title check
                        if tmdb_title:
                            tmdb_year = self.extract_movie_year(result) or None

                            # Only append year if we have one and it's not already in the title
                            normalized_title = title
                            if year and not re.search(rf"\b{year}\b", title):
                                normalized_title = f"{title} ({year})"

                            title_score = self.compare_movie_titles(normalized_title, tmdb_title, tmdb_year)
                            if title_score >= threshold_score:
                                final_title = tmdb_title
                                final_score = title_score
                                final_tmdb_id = tmdb_id
                                break
            return final_score, final_title, final_tmdb_id

        except Exception as e:
            self.logger.debug(f"TMDB validation failed for {title}: {e}")
        return 0.0, None, None

    def compare_movie_titles(self, title: str, tmdb_title: str, tmdb_year: int) -> float:
        data = [
            {
                "title": tmdb_title,
                "year": tmdb_year or None
            }
        ]
        search_best, search_scored = self.titleMatcher.match(title, data, content_type="movie")  # filename match
        if not search_best:
            search_best = {
                "score": 0
            }
        return float(search_best.get("score"))

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

    def parse_filename(self, guess: Dict[str, Any], file_path: Path, info_hash: str = None) -> Dict[str, Any]:
        """
        Parse filename with GuessIt, normalize quirks, and classify type.
        - Runs GuessIt with fallback (full path → filename only).
        - Normalizes values (lists, language objects, etc.).
        - Applies custom regex for specials, music, etc.
        """

        options = {}
        t_metadata = {}
        exts = self.get_media_extensions
        is_absolute_episode = False

        # clean filename str before guess
        filename = self.str_fix_padding(file_path.name)
        filename = self.str_sanitize(filename=filename)

        # file metadata parsing
        # f_metadata = detect_media_info(str(file_path), verify=False)

        # get metadat from organizerr
        if info_hash:
            try:
                t_metadata = utils.torrent_metadata.enrich_media_from_torrent(info_hash=info_hash.strip().lower())
                logging.info(f"Organizerr: Obtained torrent metadata: {dict(t_metadata)}")
            except Exception as e:
                logging.error(f"Organizerr: Could not obtain torrent metadata: {e}")

        clean_media = clean_media_name(filename)

        # if it's a movie
        if bool(clean_media.get("is_movie")):
            options.update({"type": "movie"})

        # if it's a series episode
        if bool(clean_media.get("is_episode")):
            options.update({"type": "episode"})
            # if it's an absolute episode number in series then tag as absolute
            if self.detect_episode_format(file_path.name) == "absolute":
                options.update({"episode_prefer_number": True})
                is_absolute_episode = True

        # if it's a music file
        if bool(clean_media.get("is_music")):
            options.update({"type": "music"})

        raw_data = dict(guessit(str(file_path), options))

        if isinstance(raw_data.get("title"), list):
            new_data = dict(guessit(str(file_path.parts[-1]), options))
            raw_data.update(new_data)

        if isinstance(raw_data.get("alternative_title"), list) or not raw_data.get("title"):
            new_data = dict(guessit(str(file_path.parts[-1]), options))
            raw_data.update(new_data)

        if isinstance(raw_data.get("season"), list) or not raw_data.get("title"):
            new_data = dict(guessit(str(file_path.parts[-1]), options))
            raw_data.update(new_data)

        if isinstance(raw_data.get("episode"), list) or not raw_data.get("title"):
            new_data = dict(guessit(str(file_path.parts[-1]), options))
            raw_data.update(new_data)

        # if it's a series but there is no episode_title parse with filename only
        if bool(clean_media.get("is_episode")) and (
                not raw_data.get("episode_title") and not guess.get("episode_title")):
            new_data = dict(guessit(str(file_path.parts[-1]), options))
            raw_data.update(new_data)

        if clean_media.get("is_episode"):
            if raw_data:
                raw_data = sanitize_guess_data(raw_data, clean_media, file_path)
            if guess:
                guess = sanitize_guess_data(guess, clean_media, file_path)
        elif clean_media.get("is_movie"):
            if raw_data.get("year") and guess.get("year") and clean_media.get("year"):
                raw_data_year = raw_data.get("year") if isinstance(raw_data.get("year"), int) else int(raw_data.get("year")) or None
                guess_year = guess.get("year") if isinstance(guess.get("year"), int) else int(guess.get("year")) or None
                clean_media_year = clean_media.get("year") if isinstance(clean_media.get("year"), int) else int(clean_media.get("year")) or None
                if raw_data_year and guess_year and clean_media_year:
                    if (guess_year == clean_media_year) and raw_data_year != clean_media_year:
                        raw_data["year"] = clean_media_year

        # enrich dict with filepath
        raw_data["file_path"] = file_path

        # Enrich raw data and fix media title if necessary
        raw_data = self.enrich_metadata2(raw_data, clean_media, options)

        # merge special types results
        if bool(clean_media.get("is_episode")):
            raw_data.update(guess)

        # merge raw_data with torrent metadata from organizerr
        if t_metadata:
            raw_data = self.enrich_with_torrent_metadata(t_metadata=t_metadata, guess=raw_data)

        # Normalize GuessIt results
        path_data: Dict[str, Any] = {}
        for k, v in raw_data.items():
            if hasattr(v, "alpha3"):  # Language object
                try:
                    path_data[k] = Language.parse(v)
                except Exception:
                    continue
            elif isinstance(v, (int, str, dt.date)):
                path_data[k] = v
            elif isinstance(v, list) and all(isinstance(_, (int, str)) for _ in v):
                path_data[k] = v[0]

        # Keep original + cleaned filename
        path_data["original_filename"] = file_path.name
        path_data["original_file_path"] = str(file_path)
        path_data["cleaned_filename"] = self._sanitize_text(file_path.stem)
        path_data["absolute_number"] = path_data.get("episode") if is_absolute_episode else None

        # --- Apply custom regex for specials ---
        if "video" in path_data.get("mimetype"):
            for rx in self.get_series_regex("SPECIAL_PREFIXES"):
                match = re.match(rx, file_path.name, re.IGNORECASE)
                if match:
                    info = match.groupdict()
                    path_data["season"] = int(path_data.get("season", 0))
                    path_data["episode"] = int(path_data.get("episode", info.get("ep") or 0))
                    path_data["type"] = "special"
                    path_data["episode_title"] = path_data.get("episode_title", info.get("title") or "Unknown")
                    path_data["title"] = path_data.get("title", info.get("show") or "Unknown")
                    return path_data

        # --- MUSIC DETECTION ---
        if "audio" in path_data.get("mimetype"):
            if file_path.suffix.lower() in exts["audio"]:
                music_patterns = [
                    r"^(?P<artist>.+?)\s*-\s*(?P<title>.+)$",
                    r"^(?P<track>\d{1,2})\s*-\s*(?P<title>.+)$",
                    r"^(?P<title>.+?)\s*\((?P<artist>.+)\)$",
                ]
                for rx in music_patterns:
                    match = re.match(rx, file_path.stem, re.IGNORECASE)
                    if match:
                        info = match.groupdict()
                        path_data["type"] = "music"
                        path_data["artist"] = path_data.get("artist", info.get("artist") or "Unknown")
                        path_data["album"] = path_data.get("album", info.get("album") or "Unknown")
                        path_data["title"] = path_data.get("title", info.get("title") or "Unknown")
                        path_data["track"] = path_data.get("track", info.get("track") or "Unknown")
                        return path_data

                # Fallback → mark as music
                path_data["type"] = "music"
                path_data["title"] = path_data.get("title") or file_path.stem
                return path_data

        # --- Default classification ---
        if "type" not in path_data or path_data["type"] not in ["episode", "special", "music", "movie", "unsorted"]:
            path_data["type"] = "unknown"

        # --- Normalize / sanitize fields ---
        for field in ["title", "artist", "album", "episode_title"]:
            if field in path_data:
                path_data[field] = self._sanitize_text(path_data[field])

        # ==============================
        # SERIES TITLE VALIDATION (TV + Anime)
        # ==============================
        if path_data["type"] in ["episode", "anime", "special"]:
            title = path_data.get("title", "").strip()

            # ❗ Skip junk/placeholder titles
            junk_titles = {"NCED", "NCOP", "OP", "ED", "SPECIAL", "OVA", "OAV"}
            if title.upper() in junk_titles or len(title) <= 2:
                title = None

            # if year is found on parsing series title or parent directory use it in the search query
            year = path_data.get("year", None) or None
            final_score, final_title, final_tvdb_id = self.validate_series_name(title, year)

            if final_score == 0 or not final_title:
                # Try parent directories for better title
                for parent in file_path.parents:
                    if parent.name.lower() not in ["complete", "season", "downloads", "tv shows", "anime shows", "data", "tdarr_temp"]:
                        e_score, e_title, e_tvdb_id = self.validate_series_name(parent.name, year)
                        if e_score > 0 or e_title:
                            path_data["title"] = e_title
                            path_data["tvdb_id"] = e_tvdb_id
                            break

                # Try search/metadata titles from clean_media
                for candidate in [clean_media.get("search_title"), clean_media.get("metadata_title")]:
                    if candidate:
                        c_score, c_title, c_tvdb_id = self.validate_series_name(candidate, year)
                        if c_score > 0:
                            path_data["title"] = c_title
                            path_data["tvdb_id"] = c_tvdb_id
                            break
            else:
                path_data["title"] = final_title
                path_data["tvdb_id"] = final_tvdb_id

        # ==============================
        # MOVIES TITLE VALIDATION
        # ==============================
        if path_data["type"] == "movie":
            title = path_data.get("title")
            alt_title = path_data.get("alternative_title")
            year = path_data.get("year") or None

            for candidate in [title, clean_media.get("search_title"), clean_media.get("metadata_title"), alt_title]:
                if candidate:
                    score, m_title, tmdb_id = self.validate_movie_name(candidate, year)
                    if score > 0:
                        path_data["title"] = m_title
                        path_data["tmdb_id"] = tmdb_id
                        break

        return path_data

    def smart_guess(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """
        Smartly determine episode information:
        - Prefer SxxExx if present
        - Then NxNN format (1x05)
        - Then absolute episode numbers (>=100), including ranges, ignoring junk numbers
        """
        filename = guess.get("original_filename", "")

        # --- 1) Detect SxxExx pattern ---
        m = re.search(r'\b[Ss](\d{1,2})[Ee](\d{1,3})\b', filename)
        if m:
            guess['season'] = int(m.group(1))
            guess['episode'] = int(m.group(2))
            guess['absolute_number'] = None
            return guess

        # --- 2) Detect NxNN format (1x05) ---
        m = re.search(r'\b(\d{1,2})[xX](\d{1,2})\b', filename)
        if m:
            guess['season'] = int(m.group(1))
            guess['episode'] = int(m.group(2))
            guess['absolute_number'] = None
            return guess

        # --- 3) Detect absolute episode numbers (>=100) ---
        # Match numbers 3-4 digits, optionally followed by v2/v3 etc.
        numbers = re.findall(r'\b(\d{3,4})(?:v\d+)?\b', filename, re.IGNORECASE)

        # Filter out junk numbers
        junk_numbers = {1080, 720, 2160, 480, 216, 10, 12, 24, 30}
        absolute_eps = [int(n) for n in numbers if int(n) >= 100 and int(n) not in junk_numbers]

        if absolute_eps:
            # Handle multiple numbers as a list
            guess['absolute_number'] = absolute_eps if len(absolute_eps) > 1 else absolute_eps[0]
            # For absolute episodes, set season to 0 and episode to the first number
            guess['season'] = 0
            guess['episode'] = absolute_eps[0]
            return guess

        # --- 4) No valid episode found ---
        guess['season'] = guess.get('season')
        guess['episode'] = guess.get('episode')
        guess['absolute_number'] = guess.get('absolute_number')
        return guess

    def _enhance_with_parent_info_v2(self, guess: Dict[str, Any], file_path: Path, max_parent_levels: int = 3) -> Dict[
        str, Any]:
        """
        Parent-directory-based enhancement for missing series/year/season info
        """

        # -------------------
        # Parent-directory-based enhancement
        # -------------------

        needs_series = guess.get('type') == 'episode' and not guess.get('title')
        needs_year = not guess.get('year')
        needs_season = guess.get('type') == 'episode' and not guess.get('season')

        if needs_series or needs_year or needs_season:
            current_path = file_path.parent
            for _ in range(max_parent_levels):
                if current_path.name:
                    parent_guess = guessit(current_path.name)

                    # Extract series title
                    if needs_series and parent_guess.get('title'):
                        guess['title'] = parent_guess['title'].replace('.', ' ').strip()
                        self.logger.info(
                            f"Extracted series name from parent directory '{current_path.name}': {guess['title']}"
                        )
                        needs_series = False

                    # Extract season number
                    if needs_season and parent_guess.get('season'):
                        guess['season'] = int(parent_guess['season'])
                        self.logger.info(
                            f"Extracted season {guess['season']} from parent directory '{current_path.name}'"
                        )
                        needs_season = False

                    # Extract year
                    if needs_year and parent_guess.get('year'):
                        year = parent_guess['year']
                        if 1900 < year <= dt.datetime.now().year + 1:
                            guess['year'] = year
                            needs_year = False

                    if not (needs_series or needs_year or needs_season):
                        break

                # Move up
                current_path = current_path.parent
                if current_path == current_path.parent:
                    break

        # -------------------
        # 4) Final normalization
        # -------------------
        guess.setdefault('title', None)
        guess.setdefault('season', None)
        guess.setdefault('episode', None)
        guess.setdefault('year', None)

        return guess

    def smart_guess_v2(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """
        Smartly determine episode information:
        1) Prefer SxxExx if present
        2) Then NxNN format (1x05)
        3) Then specials like OP/ED/OVA/SP/PV
        4) Then absolute episode number (>=10), ignoring junk numbers
        """
        filename = guess.get("original_filename", "")

        # --- 1) Detect SxxExx pattern ---
        m = re.search(r'\b[Ss](\d{1,2})[Ee](\d{1,3})\b', filename)
        if m:
            guess["season"] = int(m.group(1))
            guess["episode"] = int(m.group(2))
            return guess

        # --- 2) Detect NxNN format (1x05) ---
        m = re.search(r'\b(\d{1,2})[xX](\d{1,3})\b', filename)
        if m:
            guess["season"] = int(m.group(1))
            guess["episode"] = int(m.group(2))
            return guess

        # --- 3) Detect specials (OP, ED, OVA, SP, PV) ---
        special_patterns = {
            r'\bOP(\d*)\b': "Opening",
            r'\bED(\d*)\b': "Ending",
            r'\bOVA\b': "OVA",
            r'\bSP(ecial)?\b': "Special",
            r'\bPV\b': "Promo Video",
        }
        for pat, label in special_patterns.items():
            m = re.search(pat, filename, re.IGNORECASE)
            if m:
                number = m.group(1) if m.groups() and m.group(1) else ""
                guess["special"] = f"{label} {number}".strip()
                guess["season"] = 0
                guess["episode"] = None
                return guess

        # --- 4) Detect absolute episode number (>=10), avoid junk numbers ---
        junk_numbers = {1080, 720, 2160, 480, 216, 10}
        numbers = re.findall(r'(?<!\d)(\d{2,4})(?!\d)', filename)
        for n in numbers:
            num = int(n)
            if num >= 10 and num not in junk_numbers and not (2000 <= num <= 2099):
                guess["absolute_number"] = num
                guess["season"] = 0
                guess["episode"] = num
                return guess

        # --- fallback ---
        guess["season"] = guess.get("season", None)
        guess["episode"] = guess.get("episode", None)
        return guess

    def _normalize_anime_numbers(self, guess: Dict[str, Any], filename: str) -> Dict[str, Any]:
        """
        Normalize anime numbering: handle absolute episode numbers like 1143
        misdetected as S11E43 by guessit.
        """
        # If it's anime and has suspicious season/episode
        if guess.get("type") == "episode":
            # Extract 3-4 digit numbers from filename
            import re
            numbers = re.findall(r"\b\d{3,4}\b", filename)

            if numbers:
                num = int(numbers[0])
                # Heuristic: large numbers → absolute episode count
                if num > 100:
                    guess["absolute_number"] = num
                    guess["season"] = 0
                    guess["episode"] = num
        return guess

    def _pick_best_title(self, guess: Dict[str, Any], file_path: Path) -> str:
        """
        Decide the most reliable title between guessit output and parent directory.
        """
        search_title = guess.get("search_title")
        series_title = guess.get("title")

        # capitalize and fix media title
        series_title = self.fix_media_title(series_title)
        search_title = self.fix_media_title(search_title)

        # If series exists and looks like a real name (not generic "Complete", "Season X")
        if series_title and not any(
                word.lower() in series_title.lower() for word in ["complete", "season", "downloads"]):
            return series_title

        # If filename title exists and is different from title
        if search_title and (search_title != series_title):
            return search_title

        # Otherwise, try parent directory name
        parent_guess = guessit(file_path.parent.name)
        if parent_guess.get("title"):
            return parent_guess["title"]

        # Fallback: use filename without extension
        return file_path.stem

    def _enhance_with_parent_info(self, guess: Dict[str, Any], file_path: Path) -> Dict[str, Any]:
        """Enhance guessit results with information from parent directories for anime and specials."""
        if guess.get("type", "unknown") in ("movie", "unknown"):
            return guess

        # Determine if we need a better title (missing or generic like NCED, OVA, etc.)
        generic_titles = {"NC", "NCED", "NCOP", "OP", "ED", "OVA", "SPECIAL"}
        needs_title = (
                not guess.get("title")
                or guess["title"].upper() in generic_titles
                or guess["title"].lower() in ("episode", "special")
        )

        current_path = file_path.parent
        max_levels = 3  # search up to 3 parent folders

        skip_keywords = {"season", "complete", "downloads", "batch", "collection"}

        for _ in range(max_levels):
            folder_name = current_path.name.strip()
            if not folder_name:
                break

            # Skip meaningless folders like 'Season 1', 'Complete', etc.
            if any(k in folder_name.lower() for k in skip_keywords):
                current_path = current_path.parent
                continue

            # Run guessit on the parent directory name
            parent_guess = guessit(folder_name)

            # Extract a valid title from the folder
            if needs_title and parent_guess.get("title"):
                guess["title"] = parent_guess["title"]
                guess["parent_title"] = parent_guess["title"]
                self.logger.info(f"Extracted title from parent directory '{folder_name}': {guess['title']}")
                needs_title = False

            # Stop once a valid title is found
            if not needs_title:
                break

            # Move up
            new_parent = current_path.parent
            if new_parent == current_path:
                break
            current_path = new_parent

        return guess

    def _clean_filename(self, filename: str) -> str:
        """Remove junk characters and clean up filename for better parsing"""
        if not filename:
            return filename

        # Common junk patterns in anime/media filenames
        junk_patterns = [
            # Release group tags
            r'\[.*?\]',  # [SAD], [GroupName], [BD], etc.
            r'\(.*?\)',  # (1080p), (x265), etc.
            r'\{.*?\}',  # {Group}, {Codec}, etc.

            # Quality and codec info
            r'\b(?:BD|BR|BDRip|BluRay|Blu-Ray|WEB-DL|WEBRip|HDTV|DVDRip)\b',
            r'\b(?:x264|x265|HEVC|H264|H265|AVC|VC-1)\b',
            r'\b(?:AAC|AC3|DTS|FLAC|MP3|Opus|TrueHD|Atmos)\b',
            r'\b(?:1080p|720p|480p|2160p|4K|UHD|HD|SD)\b',
            r'\b(?:10bit|8bit|HDR|SDR|DV|DoVi)\b',

            # Release info and hashes
            r'\b(?:Dual|Dual-Audio|Multi-Audio|Multi-Sub)\b',
            r'\b(?:Complete|Uncensored|Remastered|Director\'s Cut|Extended)\b',
            r'\[[A-F0-9]{8,}\]',  # Hash codes like [84E9A4A1]
            r'\b(?:REPACK|PROPER|READNFO|NFO)\b',

            # Unwanted punctuation and separators
            r'[\[\]{}()]',  # Remove all brackets
            r' +',  # Multiple spaces
            r'^[\._\-]+|[\._\-]+$',  # Leading/trailing dots, underscores, dashes
        ]

        cleaned = filename

        # Remove junk patterns
        for pattern in junk_patterns:
            cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)

        # Remove common file extensions temporarily for cleaning
        extension = ''
        if '.' in cleaned:
            cleaned, extension = cleaned.rsplit('.', 1)
            extension = '.' + extension

        # Clean up remaining junk
        cleaned = re.sub(r'[^\w\s\-\.]', ' ', cleaned)  # Remove special chars
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Collapse multiple spaces
        cleaned = cleaned.strip()  # Trim spaces

        # Restore extension
        if extension:
            cleaned += extension

        # Additional cleaning for common anime patterns
        cleaned = self._clean_anime_specific_patterns(cleaned)

        return cleaned if cleaned.strip() else filename  # Return original if empty

    def _clean_anime_specific_patterns(self, filename: str) -> str:
        """Clean anime-specific patterns while preserving important info"""
        if not filename:
            return filename

        cleaned = filename

        # Remove common anime release patterns but keep episode numbers
        anime_patterns = [
            # Remove group names but keep content
            r'^\[[^\]]+\]\s*',  # [Group] at start
            r'\s*\[[^\]]+\]$',  # [Group] at end

            # Remove quality info but keep meaningful content
            r'\b(?:BD|BDRip|BluRay)\b',
            r'\b(?:x264|x265|HEVC)\b',
            r'\b(?:AAC|FLAC|DTS)\b',

            # Remove hashes and codes
            r'\[[A-F0-9]{8,}\]',
        ]

        for pattern in anime_patterns:
            cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)

        # Preserve episode numbers and important identifiers
        cleaned = re.sub(r'\b(?:EP|Episode|ep| episode )\s*(\d+)', r'EP\1', cleaned)
        cleaned = re.sub(r'\b(?:S|Season|s| season )\s*(\d+)', r'S\1', cleaned)

        # Clean up spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()

        return cleaned

    def _is_anime_movie(self, guess_info: Dict[str, any], filename: str) -> bool:
        """
        Heuristically determine if the given media file is an anime movie.

        Args:
            guess_info (dict): Parsed metadata from guessit or similar.
            filename (str): Original filename.

        Returns:
            bool: True if file is likely an anime movie, False otherwise.
        """
        if guess_info.get("type", "unknown") == "special":
            return False

        filename_lower = filename.lower()

        # --- 1. Explicit movie indicators in filename ---
        movie_indicators = [
            r'\bmovie\b',
            r'\bthe\s+movie\b',
            r'\bfilm\b',
            r'剧场版',  # Japanese
            r'劇場版',  # Traditional Chinese
            r'\bmovie\s*\d+\b',
            r'\bfilm\s*\d+\b',
            r'[-_\s]m\d+\b',
            r'\bpart\s*\d+\b',
            r'\bvol(?:ume)?\.?\s*\d+\b'
        ]

        for indicator in movie_indicators:
            if re.search(indicator, filename_lower, re.IGNORECASE):
                return True

        # --- 2. Guessit hints ---
        # If guessit says it's BluRay or similar format but lacks season/episode,
        # it's often a standalone anime movie.
        if (
                guess_info.get("format")
                and "bluray" in guess_info["format"].lower()
                and not any(x in guess_info for x in ["season", "episode"])
        ):
            return True

        # Guessit may directly tag the type as "movie"
        if guess_info.get("type") == "movie":
            return True

        # --- 3. Anime-specific filename patterns ---
        anime_movie_patterns = [
            r"\[.*\].* - .*?\[BD",  # Group release with BD hint
            r".*\[BD.*\].*\.(mkv|mp4|avi)$",
            r"\.movie\.",  # e.g. naruto.movie.3
            r"_movie_",  # e.g. one_piece_movie_6
            r"\banime\s*movie\b",  # explicit marker
        ]

        for pattern in anime_movie_patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                return True

        # --- 4. Additional heuristic ---
        # If filename contains "Movie" but also contains a season/episode marker, prefer TV
        if "movie" in filename_lower and re.search(r"[Ss]\d{1,2}[Ee]\d{1,2}", filename_lower):
            return False

        # --- 5. Heuristic search based on known anime movie titles
        anime_movie_keywords = self.get_anime_keywords.get('movie_anime')
        # Check against our comprehensive list
        for keyword in anime_movie_keywords:
            if keyword in filename_lower:
                return True

        return False

    def _get_series_classification_stats(self, series_title: str) -> Dict[str, int]:
        """
        Scan previously processed files in the same series folder and
        count how many were identified as anime vs tv_show.
        """
        anime_count = 0
        tv_show_count = 0

        for processed in getattr(self, "processed_files", []):
            if not processed.get("title"):
                continue
            # match by title and parent directory
            if processed["title"].lower() == series_title.lower():
                media_type = processed.get("media_type")
                if media_type == "anime":
                    anime_count += 1
                elif media_type == "tv_show":
                    tv_show_count += 1

        return {"anime": anime_count, "tv_show": tv_show_count}

    def _is_anime(self, guess_info: Dict[str, Any]) -> bool:
        title = guess_info.get("title", "").lower()
        episode_title = guess_info.get("episode_title", "").lower()
        filename = guess_info.get("original_filename", "").lower() if guess_info.get("original_filename") else ""

        # --- 1. Strong filename indicators ---
        if self.has_strong_indicator(filename=filename):
            return True

        # --- 2. GuessIt metadata hints ---
        if guess_info.get("country", "").lower() == "jp":
            return True

        genre = str(guess_info.get("genre", "")).lower()
        if "animation" in genre and ("japanese" in filename or "anime" in filename):
            return True

        # --- 3. Heuristic matching ---
        if self.is_anime_title(episode_title, 'all_anime_keywords'):
            # --- 3.1. Context-aware correction based on existing episodes ---
            stats = self._get_series_classification_stats(title)
            total = stats["anime"] + stats["tv_show"]
            if total >= 3:  # only if we have a few episodes to compare
                tv_ratio = stats["tv_show"] / total
                if tv_ratio >= 0.8:
                    self.logger.debug(f"[Context Check] {title} marked as TV show (TV ratio: {tv_ratio:.2f})")
                    return False
                elif stats["anime"] / total >= 0.8:
                    self.logger.debug(
                        f"[Context Check] {title} confirmed as anime (Anime ratio: {stats['anime'] / total:.2f})")
                    return True
        if self.is_anime_title(title, 'all_anime_keywords'):
            return True

        # --- 4. Anime release patterns (restricted to known groups) ---
        anime_patterns = [
            r"\[(HorribleSubs|Erai-raws|SubsPlease|Underwater|Commie)\]",
            r"\[BD.*?\]", r"\[HEVC.*?\]", r"\[x26[45].*?\]",
            r"\[FLAC.*?\]", r"\[Dual(\s|-)?Audio\]",
            # Keep only this " - 01" style pattern if it's near an anime keyword
            r"(?=.*\b(anime|subsplease|horriblesubs|erai|commie|underwater|fansub)\b).* - \d{1,3}(\s|\.|$)"
        ]
        if any(re.search(pat, filename, re.IGNORECASE) for pat in anime_patterns):
            return True

        # --- 5. Western content patterns ---
        western_patterns = [
            r"S\d{1,2}E\d{1,2}",
            r"\d{3,4}p",
            r"WEB[- ]?DL", r"WEBRip", r"BluRay", r"HDTV",
            r"DDP\d", "Dolby", "Atmos", "HDR", "DV"
        ]
        western_count = sum(1 for pat in western_patterns if re.search(pat, filename, re.IGNORECASE))
        if western_count >= 2:
            return False

        # --- 6. Explicit non-anime exceptions ---
        non_anime_exceptions = [
            "pixar", "disney", "dreamworks", "illumination",
            "hbo", "netflix", "amazon", "hulu", "atvp"
        ]
        if any(exc in filename for exc in non_anime_exceptions):
            return False

        return False

    # Helper function to check if something is anime
    def is_anime_title(self, title: str, keyword_type: str) -> bool:
        title_lower = title.lower()

        # Check against anime keywords
        for keyword in self.get_anime_keywords.get(keyword_type, []):
            if title_lower in keyword.lower():
                return True

        # Check Japanese naming patterns
        japanese_patterns = [
            r'\b(kimi|boku|my|koi|bunny|your) [a-z]+ no [a-z]+\b',
            r'\b(a|the|boku|kimi) [a-z]+ wa [a-z]+\b',
            r's\d{1,2}e\d{1,2}', r'ep\d{1,3}'
        ]

        # Only match anime-specific brackets, not normal years
        anime_bracket_patterns = [
            r'\[[^\]]*(BDRip|Subs|1080p|720p|TV|OVA|Movie)[^\]]*\]',
            r'\([^\)]*(BDRip|Subs|1080p|720p|TV|OVA|Movie)[^\)]*\)'
        ]

        for pattern in japanese_patterns + anime_bracket_patterns:
            if re.search(pattern, title_lower):
                return True

        return False

    def has_strong_indicator(self, filename: str) -> bool:
        strong_indicators = [
            "anime", "ova", "ona", "special",
            "fansub", "fansubs", "subsplease", "horriblesubs",
            "commie", "erai-raws", "underwater",
            "dual audio", "japanese audio"
        ]

        filename_lower = filename.lower()

        for ind in strong_indicators:
            # Match only if indicator is a separate word or within known delimiters
            pattern = rf'(?<![a-z0-9]){re.escape(ind)}(?![a-z0-9])'
            if re.search(pattern, filename_lower):
                return True
        return False

    def _extract_movie_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract movie information from guessit results"""
        return {
            'title': guess.get('title', 'Unknown'),
            'year': guess.get('year'),
            'quality': guess.get('screen_size'),
            'source': guess.get('source'),
            'resolution': guess.get('screen_size'),
            'video_codec': guess.get('video_codec'),
            'audio_codec': guess.get('audio_codec'),
            'tmdb_id': guess.get("tmdb_id")
        }

    def _extract_tv_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract TV show information from guessit results"""
        return {
            'title': guess.get('title', 'Unknown Series'),
            'season': guess.get('season'),
            'episode': guess.get('episode'),
            'episode_title': guess.get('episode_title'),
            'quality': guess.get('screen_size'),
            'source': guess.get('source'),
            'video_codec': guess.get('video_codec'),
            'tvdb_id': guess.get("tvdb_id")
        }

    def _extract_special_tv_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Special TV episode information from guessit results"""
        return {
            'title': guess.get('title', 'Unknown Series'),
            'season': guess.get('season', 0),  # special episode/season are always 0
            'episode': guess.get('episode', 0),  # special episode/season are always 0
            'episode_title': guess.get('episode_title'),
            'quality': guess.get('screen_size'),
            "anime_special_type": guess.get("anime_special_type"),
            'source': guess.get('source'),
            'video_codec': guess.get('video_codec'),
            'tvdb_id': guess.get("tvdb_id"),
            "media_type": guess.get("type"),
            "year": None,
        }

    def _extract_unsorted_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Unsorted media information from guessit results"""
        return {
            'title': guess.get('title', 'Unknown'),
            'year': guess.get('year'),
            'quality': guess.get('screen_size'),
            'source': guess.get('source'),
            'resolution': guess.get('screen_size'),
            'video_codec': guess.get('video_codec'),
            'audio_codec': guess.get('audio_codec'),
            'stashdb_id': guess.get("stashdb_id")
        }

    def _extract_anime_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract anime information from guessit results"""
        return {
            'title': guess.get('title', 'Unknown Anime'),
            'season': guess.get('season'),
            'episode': guess.get('episode'),
            'episode_title': guess.get('episode_title'),
            'release_group': guess.get('release_group'),
            'quality': guess.get('screen_size'),
            'source': guess.get('source'),
            'video_codec': guess.get('video_codec'),
            'tvdb_id': guess.get("tvdb_id")
        }

    def _normalize_movie_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize movie metadata for consistency."""
        try:
            # guess["title"] = self._sanitize_text(guess.get("title")) or "Unknown Movie"
            guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Movie"

            # remove Non-ascii chars from title and capitalize title
            # guess["title"] = self.str_title_case(self.str_scenify2(guess.get("title")))

            year = guess.get("year")
            try:
                guess["year"] = int(year) if year else None
            except (ValueError, TypeError):
                guess["year"] = None

            guess["quality"] = guess.get("screen_size")
            guess["source"] = guess.get("source")
            guess["resolution"] = guess.get("screen_size")
            guess["video_codec"] = guess.get("video_codec")
            guess["audio_codec"] = guess.get("audio_codec")
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")
        return guess

    def _normalize_tv_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize TV show metadata for consistency."""
        try:
            # guess["title"] = self._sanitize_text(guess.get("title")) or "Unknown Series"
            # guess["episode_title"] = self._sanitize_text(guess.get("episode_title")) or None
            guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Series"
            # guess["episode_title"] = self.str_sanitize(guess.get("episode_title")) or None

            # remove Non-ascii chars from title only and capitalize title
            # normalized_title = self.str_scenify2(guess.get("title"))
            normalized_title = self.str_fix_padding(guess.get("title"))
            guess["title"] = self.str_title_case(normalized_title)
            # str_fix_padding
            # guess["episode_title"] = self.str_title_case(self.str_scenify2(guess.get("episode_title")))

            # Season number
            season = guess.get("season")
            try:
                guess["season"] = int(season) if season else 1
            except (ValueError, TypeError):
                guess["season"] = 1

            # Episode number
            episode = guess.get("episode")
            try:
                guess["episode"] = int(episode) if episode else 1
            except (ValueError, TypeError):
                guess["episode"] = 1
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")

        return guess

    def _normalize_special_tv_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Special TV episode metadata for consistency."""
        try:
            # guess["title"] = self._sanitize_text(guess.get("title")) or "Unknown Series"
            # guess["episode_title"] = self._sanitize_text(guess.get("episode_title")) or None
            guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Series"
            # guess["episode_title"] = self.str_sanitize(guess.get("episode_title")) or None

            # remove Non-ascii chars from title only and capitalize title
            # normalized_title = self.str_scenify2(guess.get("title"))
            normalized_title = self.str_fix_padding(guess.get("title"))
            guess["title"] = self.str_title_case(normalized_title)
            # str_fix_padding
            # guess["episode_title"] = self.str_title_case(self.str_scenify2(guess.get("episode_title")))

            # Season number, special episode/season numbers are always 0
            season = guess.get("season")
            try:
                guess["season"] = int(season) if season else 0
            except (ValueError, TypeError):
                guess["season"] = 0

            # Episode number
            episode = guess.get("episode")
            try:
                guess["episode"] = int(episode) if episode else 0
            except (ValueError, TypeError):
                guess["episode"] = 0
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")

        return guess

    def _normalize_unsorted_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Unsorted media metadata for consistency."""
        """Normalize movie metadata for consistency."""
        try:
            guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Movie"
            year = guess.get("year")
            try:
                guess["year"] = int(year) if year else None
            except (ValueError, TypeError):
                guess["year"] = None
            guess["quality"] = guess.get("screen_size")
            guess["source"] = guess.get("source")
            guess["resolution"] = guess.get("screen_size")
            guess["video_codec"] = guess.get("video_codec")
            guess["audio_codec"] = guess.get("audio_codec")
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")
        return guess

    def _normalize_anime_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize anime metadata for consistency (inherits TV logic)."""
        try:
            guess = self._normalize_tv_info(guess)
            # guess["title"] = self._sanitize_text(guess.get("title")) or "Unknown Anime"
            # guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Anime"

            # remove Non-ascii chars from title only and capitalize title
            # guess["title"] = self.str_title_case(self.str_scenify2(guess.get("title")))
            # guess["episode_title"] = self.str_title_case(self.str_scenify2(guess.get("episode_title")))

            guess["release_group"] = guess.get("release_group")
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")
        return guess

    def _normalize_music_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize music metadata for consistency.
        Ensures artist, album, title, and track fields always exist and are sanitized.
        """
        try:
            guess["artist"] = self._sanitize_text(guess.get("artist")) or "Unknown Artist"
            guess["album"] = self._sanitize_text(guess.get("album")) or "Unknown Album"
            guess["title"] = self._sanitize_text(guess.get("title")) or "Unknown Track"

            guess["artist"] = self.str_sanitize(guess.get("artist")) or "Unknown Artist"
            guess["album"] = self.str_sanitize(guess.get("album")) or "Unknown Album"
            guess["title"] = self.str_sanitize(guess.get("title")) or "Unknown Track"

            # Normalize track number
            track = guess.get("track")
            try:
                guess["track"] = int(track) if track else 1
            except (ValueError, TypeError):
                guess["track"] = 1

            # Normalize disc number
            disc = guess.get("disc")
            try:
                guess["disc"] = int(disc) if disc else 1
            except (ValueError, TypeError):
                guess["disc"] = 1

            # Year normalization
            year = guess.get("year")
            try:
                guess["year"] = int(year) if year else None
            except (ValueError, TypeError):
                guess["year"] = None
        except (ValueError, TypeError) as exp:
            logging.error(f"Exception: {exp}")
        return guess

    def _extract_music_info(self, guess: Dict[str, Any]) -> Dict[str, Any]:
        """Extract music information from guessit results"""
        return {
            'artist': guess.get('artist', 'Unknown Artist'),
            'album': guess.get('album', 'Unknown Album'),
            'title': guess.get('title', 'Unknown Title'),
            'track': guess.get('track'),
            'disc': guess.get('disc'),
            'year': guess.get('year'),
            'bitrate': guess.get('bitrate'),
            'codec': guess.get('audio_codec')
        }

    def _fallback_identification(self, file_path: Path) -> str:
        """Fallback identification using file extension"""
        extension = file_path.suffix.lower()
        video_extensions = self.get_media_extensions.get("video")
        audio_extensions = self.get_media_extensions.get("audio")
        image_extensions = self.get_media_extensions.get("image")

        if extension in video_extensions:
            return 'movie'  # Default to movie for video files
        elif extension in audio_extensions:
            return 'music'
        elif extension in image_extensions:
            return 'image'
        else:
            return 'unknown'

    def get_mime_type(self, file_path: Path) -> str:
        """Get MIME type using mimetypes library (fallback without magic)"""
        mime_type, _ = mimetypes.guess_type(str(file_path))
        return mime_type or 'application/octet-stream'
