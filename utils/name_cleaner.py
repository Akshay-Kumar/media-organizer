import json
import logging
import os
import re
from pathlib import Path
from knowit import api
from utils.media_extensions import get_media_extensions

JUNK_JSON_FILE = "junk_words.json"
RELEASE_GROUPS_FILE = "release_groups.json"


# --- Helpers ---

def load_release_groups():
    """Load release groups from release_groups.json and return as a unique lowercase set."""
    if os.path.exists(RELEASE_GROUPS_FILE):
        with open(RELEASE_GROUPS_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    return set(map(str.lower, data))
            except Exception as e:
                logging.warning(f"Failed to load release_groups.json, using defaults: {e}")
    return set()


def load_json_set(file):
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_json_set(data, file):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(list(data), f, indent=2, ensure_ascii=False)


learned_junk_words = load_json_set(JUNK_JSON_FILE)


# --- Main cleaner ---
def clean_media_name(filename):
    """
    Media name cleaner (hybrid static + self-learning junk removal)
    """
    global learned_junk_words
    video_extensions = get_media_extensions().get("video")
    audio_extensions = get_media_extensions().get("audio")

    raw = os.path.splitext(os.path.basename(filename))[0]  # filename
    ext = os.path.splitext(os.path.basename(filename))[1]  # file extension

    # normalize separators
    norm = re.sub(r'[_\.]+', ' ', raw)
    norm = re.sub(r'\s+', ' ', norm).strip()

    version = None
    season = None
    episodes = []
    year = None
    special_type = None
    special_number = None
    se_match = None
    is_movie = False
    is_music = False
    is_episode = False

    # --- 1) Smart Movie / OVA / Special detection ---
    movie_patterns = [
        r'\b(?:The\s+)?Movie\s+(\d{1,3})\b',
        r'\b(OVA|OAV|Special)\b[\s\.:-]*(\d{1,3})?',
    ]
    for pattern in movie_patterns:
        m = re.search(pattern, norm, re.IGNORECASE)
        if m:
            special_type = m.group(1).title() if m.group(1) else "Movie"
            if len(m.groups()) > 1 and m.group(2):
                try:
                    special_number = int(m.group(2))
                except:
                    pass
            if "Movie" in pattern or special_type == "Movie":
                is_movie = True
            se_match = m.group(0)
            break

    # ✅ Handle "Part" smartly (avoid treating episode parts as movies)
    m = re.search(r'\bPart[\s\-]*(\d{1,2})\b', norm, re.IGNORECASE)
    if m:
        has_episode_number = bool(re.search(r'[-_\s](\d{1,3})(?:\s*\(|$)', norm))
        has_movie_word = bool(re.search(r'\bMovie\b', norm, re.IGNORECASE))
        has_episode_keywords = bool(re.search(r'\b(Episode|Ep|S\d+E\d+|x\d+)\b', norm, re.IGNORECASE))

        if has_movie_word and not (has_episode_number or has_episode_keywords):
            # e.g., "Naruto Shippuden The Movie Part 1"
            special_type = "Movie"
            try:
                special_number = int(m.group(1))
            except:
                pass
            is_movie = True
            se_match = m.group(0)
        else:
            # Episode with Part in title (e.g., "Part 1" in Kakashi Chronicles)
            special_type = None
            try:
                special_number = int(m.group(1))
            except:
                special_number = None

    # --- 2) Year detection (safe and codec-aware) ---
    if not is_movie:
        m = re.search(r'\b(19\d{2}|20\d{2})\b', norm)
        if m:
            year = m.group(1)

            # Compile episode-like pattern (but not codecs)
            episode_like_pattern = re.compile(
                r'\b('
                r'Episode|Ep|'  # Episode keywords
                r'S\d{1,2}E\d{1,2}|'  # S01E01
                r'(?<!x)(\d{1,2}x\d{1,2})'  # 1x05 but not part of x265
                r')\b',
                re.IGNORECASE
            )

            # Only treat as movie if no SxxExx or episode-like pattern
            if not re.search(r'[Ss]\d{1,2}[Ee]\d{1,2}', norm):
                if not episode_like_pattern.search(norm):
                    is_movie = True

    # --- 3) Episode detection ---
    if not is_movie:
        original_se_match = se_match

        # Multi-episode S02E01E02
        m = re.search(r'[Ss](\d{1,2})', norm)
        if m:
            season = int(m.group(1))
            eps = re.findall(r'[Ee](\d{1,3})', norm)
            if eps:
                episodes = [int(e) for e in eps]
                se_match = "S{}E{}".format(season, "E".join(eps))

        # 1x01 format
        if not episodes:
            m = re.search(r'\b(\d{1,2})[xX](\d{1,2})(?:[-–](\d{1,2}))?\b', norm)
            if m:
                season = int(m.group(1))
                episodes.append(int(m.group(2)))
                if m.group(3):
                    episodes.append(int(m.group(3)))
                se_match = m.group(0)

        # "Episode 01" style
        if not episodes:
            m = re.search(r'\b[Ee]pisode[\s\-]*(\d{1,3})(?:[-–](\d{1,3}))?\b', norm)
            if m:
                episodes.append(int(m.group(1)))
                if m.group(2):
                    episodes.append(int(m.group(2)))
                se_match = m.group(0)

        # ✅ Improved absolute episode fallback (handles " - 01 (...)" and similar)
        if not episodes:
            m = re.search(r'[-_\s](\d{1,3})(?:\s*\(|$)', norm)
            if m:
                ep_num = m.group(1)
                # Ignore "part 2", "chapter 3", etc.
                if not re.search(r'\b(part|chapter)\s*' + re.escape(ep_num) + r'\b', norm, re.IGNORECASE):
                    if not re.match(r'^(19|20)\d{2}$', ep_num):
                        episodes = [int(ep_num)]
                        se_match = ep_num
                        is_episode = True

        if special_type and not episodes and original_se_match:
            se_match = original_se_match

    # --- 4) Cleaning ---
    cleaned = raw

    # ✅ Remove only square bracketed tags [ ... ]
    cleaned = re.sub(r'\[[^\]]*\]', ' ', cleaned)

    # Keep parentheses () and braces {} intact for now
    # (optional – if you really want to remove them too, add similar lines)

    # Normalize spacing
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # --- Load dynamic release groups ---
    custom_release_groups = load_release_groups()

    # --- Static release group names ---
    static_release_groups = {
        "eztv", "rarbg", "yify", "yts", "horriblesubs", "subsplease", "ethel", "psa", "ntb",
        "sva", "tbs", "fqm", "avi", "lol", "dimension", "immerse", "killers", "bae", "avs",
        "ches", "qube", "monkee", "tgx", "qxr", "rtx", "vyndros", "joy", "ettv", "rarbgx",
        "galaxyrg", "sparrow", "mkvhub", "ganool", "evo", "don", "trollhd", "uplg", "trollu",
        "ihd", "deimos", "solstice", "sys", "viethd", "frenchteam", "otr", "greeksubs",
        "xmf", "klaxxon", "w4f", "nimitz", "fov", "visum", "rartv", "warlord", "afm72", "iceblue"
    }

    # --- Combine (only unique ones) ---
    all_release_groups = static_release_groups.union(custom_release_groups)

    # --- 4a) Static quality + release group patterns ---
    quality_patterns = [
        # 0 -> Resolution / quality
        r'\b(?:4k|2160p|1080p|720p|480p|uhd)\b',
        r'\b(?:hdr|dv|dolby[\s-]*vision|10bit)\b',

        # 2 -> Source / rip type
        r'\b(?:bluray|bdrip|webrip|web[- ]?dl|hdtv|dvdrip|remux|brrip|web|bd)\b',

        # 3 -> Codecs
        r'\b(?:x264|x265|h264|h265|hevc|av1)\b',

        # 4 -> Audio
        r'\b(?:aac|ac3|dts|flac|mp3|opus|vorbis|(avc))\b',
        r'\b(?:5\.1|7\.1|2\.0|stereo|surround)\b',

        # 6 -> Multi-audio / subs
        r'\b(?:dual[\s-]*audio|multi[\s-]*audio|multi)\b',
        r'\b(?:ita|eng|jpn|ger|fre|spa|subbed|dubbed)\b',

        # 8 -> Misc
        r'\b(?:complete|proper|repack|remastered|rip)\b',

        # Release groups (fixed single regex!)
        # r'\b(?:eztv|rarbg|yify|yts|horriblesubs|subsplease|ethel|psa|ntb|sva|tbs|fqm|avi|lol|dimension|immerse'
        # r'|killers|bae|avs|ches|qube|monkee|tgx|qxr|rtx|vyndros|joy|ettv|rarbgx|galaxyrg|sparrow|mkvhub|ganool|evo'
        # r'|don|trollhd|uplg|trollu|ihd|deimos|solstice|sys|viethd|frenchteam|otr|greeksubs|xmf|klaxxon|w4f|nimitz|fov'
        # r'|visum|rartv|warlord|afm72)\b',

        # 9 -> Release groups (fixed single regex!)
        rf"\b(?:{'|'.join(sorted(all_release_groups))})\b",

        # 10 -> Resolution-style leftovers
        r'\b\d{3,4}[xX]\d{3,4}\b',

        # 11 -> Hashes
        r'\b[a-fA-F0-9]{8}\b',

        # 12 -> Surround formats
        r'\b(?:AAC\d(?:\.\d)?)\b',
        r'\b(?:DDP\d(?:\.\d)?)\b',
        r'\b(?:Atmos)\b',

        # 15 -> Extra cam/source variations and junk
        r'\b(?:hdcam|hdtc|camrip|telesync|ts|tc|line[\s-]?audio|hq)\b',
        r'\b(?:v\d{1,2})\b',  # V2, V3, etc.
        r'\b(?:hindi[-\s]?line|tamil[-\s]?line|telugu[-\s]?line|korean[-\s]?line|english[-\s]?line)\b',
    ]

    # Remove standalone language words
    cleaned = re.sub(r'\b(?:hindi|telugu|tamil|kannada|malayalam|english|japanese|korean|dual|multi)\b', ' ',
                     cleaned, flags=re.IGNORECASE)

    for pattern in quality_patterns:
        cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)

    # --- 4b) Self-learning junk words ---
    for junk in learned_junk_words:
        cleaned = re.sub(r'\b' + re.escape(junk) + r'\b', ' ', cleaned, flags=re.IGNORECASE)

    # --- Language & Line cleanup ---
    # Remove standalone language markers
    cleaned = re.sub(
        r'\b(?:hindi|telugu|tamil|kannada|malayalam|english|japanese|korean|chinese|dual|multi)\b',
        ' ',
        cleaned,
        flags=re.IGNORECASE,
    )

    # Remove language + line combo (e.g., "Hindi Line", "Tamil-Line")
    cleaned = re.sub(
        r'\b(?:hindi|telugu|tamil|kannada|malayalam|english|japanese|korean|chinese)\s*[- ]?\s*line\b',
        ' ',
        cleaned,
        flags=re.IGNORECASE,
    )

    # Remove leftover or standalone 'Line' anywhere after language cleanup
    cleaned = re.sub(r'(?:[-_.\s]|^)(line)(?:[-_.\s]|$)', ' ', cleaned, flags=re.IGNORECASE)

    # Remove remaining trailing junk (like unknown release groups)
    m = re.search(r'[-\s]*\[(.*?)\]$', cleaned)
    if m:
        junk_word = m.group(1).strip()
        cleaned = re.sub(r'[-\s]*\[' + re.escape(junk_word) + r'\]$', '', cleaned)
        if junk_word.lower() not in (w.lower() for w in learned_junk_words):
            learned_junk_words.add(junk_word)

    # Replace separators, clean extra spaces
    cleaned = re.sub(r'[\._-]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Generic trailing release group remover
    # cleaned = re.sub(r'[-\s]+[A-Za-z0-9]{2,}$', '', cleaned)
    # Only remove if it looks like a release tag / hash: usually uppercase letters/numbers only
    cleaned = re.sub(r'[-\s]+\[[A-Za-z0-9]{2,}\]$', '', cleaned)  # for [HASH]
    cleaned = re.sub(r'[-\s]+[A-Z0-9]{2,}$', '', cleaned)  # for uppercase-only release group

    # --- 5) Final cleanup ---
    search_title = cleaned

    # Keep "Chapter 1" or "Part 1" patterns as part of title
    chapter_match = re.search(r'\b(Chapter|Part)\s*[-]?\s*(\d{1,3})\b', cleaned, re.IGNORECASE)
    if chapter_match:
        chapter_text = f"{chapter_match.group(1).title()} {chapter_match.group(2)}"
    else:
        chapter_text = None

    # Remove "Movie 01" or episode-like patterns only if not part of title
    if is_movie:
        search_title = re.sub(r'\b(?:The\s+)?Movie\s+\d+\b', '', search_title, flags=re.IGNORECASE)
    else:
        # ✅ Improved episode removal (handles " - 01 (...)" etc.)
        episode_patterns = [
            r'S\d{1,2}E\d{1,2}(?:E\d{1,2})*',
            r'\d{1,2}x\d{1,2}(?:-\d{1,2})?',
            r'\bEpisode\s+\d{1,3}(?:-\d{1,3})?\b',
            r'[-_\s]+(?:\d{1,3})(?:\s*\(|$)',  # catch " - 01 (" and similar
        ]
        for pattern in episode_patterns:
            search_title = re.sub(pattern, ' ', search_title, flags=re.IGNORECASE)
        search_title = re.sub(r'\s+', ' ', search_title).strip()

    # Remove year if present
    if year:
        search_title = re.sub(r'\b' + re.escape(year) + r'\b', '', search_title)

    # ✅ Remove version numbers like "v2", "v3" etc.
    search_title = re.sub(r'\bv\d{1,2}\b', '', search_title, flags=re.IGNORECASE).strip()

    # Reattach Chapter info if lost
    if chapter_text and chapter_text.lower() not in search_title.lower():
        search_title = f"{search_title} {chapter_text}".strip()

    # --- 6) Final fallback ---
    if not search_title or len(search_title) < 2 or search_title.isdigit():
        search_title = cleaned

    # ✅ Remove empty or unmatched parentheses/brackets/braces
    for target_name, target_value in [('search_title', search_title), ('cleaned', cleaned)]:
        target_value = re.sub(r'\(\s*\)', '', target_value)  # empty parentheses
        target_value = re.sub(r'\[\s*\]', '', target_value)  # empty square brackets
        target_value = re.sub(r'\{\s*\}', '', target_value)  # empty curly braces
        target_value = re.sub(r'\s*[\)\]\}]+', '', target_value)  # stray closing
        target_value = re.sub(r'[\(\[\{]+\s*', '', target_value)  # stray opening
        target_value = re.sub(r'\s+', ' ', target_value).strip()

        if target_name == 'search_title':
            search_title = target_value
        else:
            cleaned = target_value

    # Normalize title
    search_title = re.sub(r'\s+', ' ', search_title).strip().title()

    # Extract version
    version_match = re.search(r'[Ss]\d{1,2}[Ee]\d{1,3}v(\d+)', filename)
    if version_match:
        version = int(version_match.group(1))

    # Save updated learned junk words
    save_json_set(learned_junk_words, JUNK_JSON_FILE)

    # correct media_type in case is_movie=True but file extension is not video
    if is_movie and ext not in video_extensions:
        is_movie = False

    if not is_movie and ext in video_extensions:
        is_episode = True

    if ext in audio_extensions:
        is_music = True

    return {
        'search_title': search_title,
        'metadata_title': cleaned,
        'season': season,
        'episodes': episodes if episodes else None,
        'year': year,
        'special_type': special_type,
        'special_number': special_number,
        'filename': filename,
        'version': version,
        'is_movie': is_movie,
        'is_music': is_music,
        'is_episode': is_episode
    }


def clean_title(name: str) -> str:
    # remove extension
    name = re.sub(r"\.\w{2,4}$", "", name)

    # remove junk
    junk_patterns = [
        r"\b\d{3,4}p\b", r"\b\d+bit\b", r"\b\d+ch\b",
        r"x264|x265|h\.?264", r"blu[- ]?ray", r"web[- ]?dl",
        r"hdtv", r"dvdrip", r"bdrip",
        r"uncut|extended|remaster(ed)?",
    ]
    for pat in junk_patterns:
        name = re.sub(pat, "", name, flags=re.IGNORECASE)

    # normalize
    name = re.sub(r"[\._]", " ", name)
    name = re.sub(r"\s{2,}", " ", name).strip()

    # split on AKA / alias
    name = re.split(r"\b(?:a\.?k\.?a\.?|aka)\b", name, flags=re.IGNORECASE)[0].strip()

    return name.title()


def sanitize_filename(name: str) -> str:
    """Remove extra spaces, normalize string."""
    return re.sub(r'\s+', ' ', name).strip()


# --- Wrapper for Knowit ---
def knowit_parse(filepath: str) -> dict:
    """
    Correct Knowit parser using the proper API.
    knowit v0.5.11+ uses api.know() or knowit.know() function.
    """
    try:
        # Method 1: Using knowit.api.know()
        info = api.know(filepath)
        return dict(info) if info else {}
    except Exception as e:
        try:
            # Method 2: Alternative import style
            import knowit
            info = knowit.know(filepath)
            return dict(info) if info else {}
        except Exception as e2:
            logging.warning(f"[Knowit] Failed to parse {filepath}: {e} | {e2}")
            return {}


# --- Main knowit parser ---
def parse_media_file(file_path: Path) -> dict:
    """
        1. Knowit file parser
    """
    try:
        # Step1: Knowit parsing (CORRECTED)
        knowit_data = knowit_parse(str(file_path))
    except Exception as e:
        logging.warning(f"[knowit] Failed to parse {file_path}: {e}")
        knowit_data = {}

    # Step 2: structure results
    merged = {
        "filename": file_path.name,
        "title": knowit_data.get("title"),
        "year": knowit_data.get("year"),
        "type": knowit_data.get("type"),
        "season": knowit_data.get("season"),
        "episodes": knowit_data.get("episodes"),
        "special_type": knowit_data.get("special_type"),
        "special_number": knowit_data.get("special_number"),
    }

    # Step 3: Add Knowit enhancements (resolution, source, codecs, etc.)
    # Knowit uses different field names - adjust accordingly
    merged.update({
        "resolution": knowit_data.get("resolution"),
        "source": knowit_data.get("source"),
        "video_codec": knowit_data.get("video_codec"),
        "audio_codec": knowit_data.get("audio_codec"),
        "audio_languages": knowit_data.get("audio_languages"),
        "subtitle_languages": knowit_data.get("subtitle_languages"),
        "container": knowit_data.get("container") or knowit_data.get("extension"),
        "duration": knowit_data.get("duration"),
        "bit_rate": knowit_data.get("bit_rate"),
    })

    # Step 6: Normalize type
    if merged.get("type"):
        merged["type"] = str(merged["type"]).rstrip("s").lower()

    # Step 7: Post-clean title
    title = merged.get("title")
    if title:
        title = sanitize_filename(title)
        title = re.sub(r'\s+', ' ', title).strip().title()
        merged["title"] = title
    else:
        merged["title"] = "Unknown"

    return merged


def validate_season_and_episode_number(guess_data: dict, clean_media: dict, file_path=None):
    if not guess_data or not clean_media:
        return guess_data

    def is_valid_season(value):
        return isinstance(value, int) and 0 < value < 100

    def is_valid_episode(value):
        return isinstance(value, int) and 0 < value < 3000

    # Normalize multi-episode guesses
    if isinstance(guess_data.get("episode"), list):
        guess_data["episode"] = int(guess_data["episode"][0])

    season = guess_data.get("season")
    clean_season = clean_media.get("season")

    # Drop invalid or "year-like" seasons
    if not is_valid_season(season) or (season and 1900 <= season <= 2100):
        season = None

    if clean_season and (not season or season != int(clean_season)):
        season = int(clean_season)

    episode = guess_data.get("episode")
    clean_episodes = clean_media.get("episodes") or []
    clean_episodes.sort()
    # clean_episode = int(clean_episodes[0]) if clean_episodes else None

    if not is_valid_episode(episode):
        episode = None

    if clean_episodes and (not episode or episode not in clean_episodes):
        episode = clean_episodes[0]

    # Try extracting leading episode number from filename if missing
    if not episode and file_path:
        match = re.match(r'^\D*(\d{1,3})\D', str(file_path.name))
        if match:
            episode = int(match.group(1))

    if not season:
        season = 1

    guess_data["season"] = season
    guess_data["episode"] = episode
    return guess_data


def get_series_parent_name(file_path: Path) -> str:
    """
    Given a Path object to a media file, return the most likely series
    directory name (string), skipping generic folders like 'Downloads',
    'Complete', or 'Season 01'.
    """
    generic_dirs = {
        'downloads', 'download', 'complete', 'finished', 'temp',
        'incoming', 'torrents', 'season', 'seasons', 'shows',
        'tv', 'videos', 'media', 'anime', 'movies'
    }

    season_pattern = re.compile(r'(?i)^season\s*\d+$')
    current = file_path.parent

    while current != current.parent:
        name = current.name.strip().lower()
        if name not in generic_dirs and not season_pattern.match(name):
            return current.name
        current = current.parent

    # fallback: just return immediate parent name
    return file_path.parent.name


def validate_episode_title(guess_data: dict, file_path: Path) -> dict:
    title = str(guess_data.get("title", "")).strip()
    ep_title = str(guess_data.get("episode_title", "")).strip()

    if not ep_title:
        return guess_data

    ep_lower = ep_title.lower()

    JUNK_TITLES = {"v2", "final", "end", "new", "extra", "alt"}
    if len(ep_lower) <= 3 or ep_lower in JUNK_TITLES:
        guess_data.pop("episode_title", None)
        return guess_data

    # Remove text in parentheses (like "(English Subtitles)")
    ep_title = re.sub(r"\s*\([^)]*\)\s*", "", ep_title).strip()
    ep_lower = ep_title.lower()

    if ep_lower in title.lower():
        guess_data.pop("episode_title", None)
        return guess_data

    if re.fullmatch(r"(ep(isode)?\s*\d+|part\s*\d+)", ep_lower):
        guess_data.pop("episode_title", None)
        return guess_data

    if re.search(r"(1080p|720p|x26[45]|hevc|aac|dub|sub|dual\s*audio)", ep_lower):
        guess_data.pop("episode_title", None)
        return guess_data

    if re.search(r"(subtitles?|dubbed|remastered|extended|uncensored)", ep_lower):
        guess_data.pop("episode_title", None)
        return guess_data

    if re.fullmatch(r"\d+", ep_lower):
        guess_data.pop("episode_title", None)
        return guess_data

    # Allow descriptive titles, reject short numeric-heavy ones
    if len(ep_lower) < 15 and re.search(r"\d{2,}", ep_lower):
        guess_data.pop("episode_title", None)
        return guess_data

    if not re.match(r"^[\w\s'“”‘’\-:,.!()&]+$", ep_title):
        guess_data.pop("episode_title", None)
        return guess_data

    parent_directory = get_series_parent_name(file_path)
    if ep_lower in parent_directory.lower():
        guess_data.pop("episode_title", None)
        return guess_data

    guess_data["episode_title"] = ep_title
    return guess_data


def sanitize_guess_data(data: dict, clean_media: dict, file_path: Path) -> dict:
    data = validate_season_and_episode_number(data, clean_media)
    data = validate_episode_title(data, file_path)
    return data
