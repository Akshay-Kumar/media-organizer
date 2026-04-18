import os
import re
from guessit import guessit

ANIME_SPECIAL_KEYWORDS = {
    "op": "OP",
    "ed": "ED",
    "ova": "OVA",
    "ona": "ONA",
    "special": "SPECIAL",
    "ncop": "OP",
    "nced": "ED",
    "nc": "SPECIAL",
}
GENERIC_PARENT_NAMES = {
    "season", "downloads", "download", "complete", "episodes",
    "tvshows", "tv shows", "episode", "nc", "op", "ed", "ova", "ona",
    "nced", "ncop",
}


def is_valid_title(title: str) -> bool:
    """Check if a guessed title is meaningful (not numeric, generic, or junk)."""
    if not title:
        return False
    title_clean = title.strip().lower()

    if len(title_clean) < 2:
        return False
    if title_clean in GENERIC_PARENT_NAMES:
        return False
    if title_clean.startswith("season"):
        return False
    if title_clean.isdigit():
        return False
    if re.match(r"^(s\d+|e\d+|1080p|720p|480p|part\d+)$", title_clean):
        return False
    return True


def detect_anime_special_type(filepath: str) -> str | None:
    """Detect anime special type (OP/ED/OVA/etc.) from filename and parent directories."""
    path_parts = os.path.normpath(filepath).split(os.sep)
    tokens = set()
    for part in path_parts:
        for token in re.split(r"[^a-zA-Z0-9]+", part.lower()):
            if token:
                tokens.add(token)

    for token in tokens:
        for keyword, mapped in ANIME_SPECIAL_KEYWORDS.items():
            # if token == keyword or token.startswith(keyword):
            #    return mapped
            if token == keyword:
                return mapped
    return None


def infer_series_title_from_parents(filepath: str) -> str | None:
    """
    Walk up all directories and pick the deepest valid anime title.
    """
    path_parts = os.path.normpath(filepath).split(os.sep)
    valid_titles = []

    # Walk up from the parent folder upwards
    for i in range(len(path_parts) - 2, -1, -1):
        folder = path_parts[i]
        lower_folder = folder.lower()

        # Skip clearly generic or junk folders
        if lower_folder in GENERIC_PARENT_NAMES or lower_folder.startswith("season"):
            continue

        guess = guessit(folder)
        possible_title = guess.get("title")
        if is_valid_title(possible_title):
            cleaned = re.sub(r"[._]+", " ", possible_title).strip()
            valid_titles.append(cleaned)

    # Return the deepest valid one (closest to the file but not junk)
    if valid_titles:
        return valid_titles[0]  # first found valid one from bottom up

    return None


def parse_media_info(filepath: str) -> dict:
    """
    Parse media info using guessit with:
      - anime title fallback from parent directories (multi-level),
      - episode title from filename,
      - anime special detection.
    """
    filename = os.path.basename(filepath)
    info = guessit(filename)

    # Detect anime title from parent directories
    title = info.get("title")
    if not title:
        parent_title = infer_series_title_from_parents(filepath)
        if parent_title and is_valid_title(parent_title):
            info["title"] = parent_title

    # Extract clean episode title
    episode_title = info.get("episode_title")
    if not episode_title:
        base_name = os.path.splitext(filename)[0]
        base_name_clean = re.sub(r"[._]+", " ", base_name).strip()
        if info.get("title") and base_name_clean.lower().startswith(info["title"].lower()):
            base_name_clean = base_name_clean[len(info["title"]):].strip(" -_")
        episode_title = base_name_clean
        info["episode_title"] = episode_title

    # Detect anime special
    anime_special_type = detect_anime_special_type(filepath)
    if anime_special_type:
        info["anime_special_type"] = anime_special_type
        info["type"] = "special"

    # test block to derive season number and episode numbers from file path
    if info["type"] in ["anime", "tv_show", "special", "episode"]:
        if info.get("season", None) is None:
            season_info = guessit(filepath)
            if season_info.get("season"):
                info["season"] = season_info.get("season")
        if info.get("episode", None) is None:
            episode_info = guessit(filepath)
            if episode_info.get("episode"):
                info["episode"] = episode_info.get("episode")

    return info


def parse_path(path: str) -> dict:
    """Unified entry point for file or directory."""
    if os.path.isfile(path):
        return parse_media_info(path)
    else:
        raise FileNotFoundError(f"Path not found: {path}")
