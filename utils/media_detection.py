import os
import re
import json
import subprocess
import logging
from typing import Optional, Dict, Any


logger = logging.getLogger(__name__)


# -------------------------------
# Core filename-based detection
# -------------------------------
def _detect_from_filename(file_path: str) -> Dict[str, Any]:
    """
    Detect media type, title, season, episode, and year based on filename.
    """
    filename = os.path.basename(file_path)
    name, ext = os.path.splitext(filename)
    ext = ext.lstrip(".").lower()

    info = {
        "file": filename,
        "media_type": None,
        "title": None,
        "season": None,
        "episode": None,
        "episode_title": None,
        "year": None,
        "extension": ext
    }

    # Common patterns for episodes
    episode_patterns = [
        r"[Ss](\d{1,2})[Ee](\d{1,2})",    # S01E02
        r"(\d{1,2})x(\d{1,2})"            # 1x02
    ]

    # Detect season and episode
    for pat in episode_patterns:
        m = re.search(pat, name)
        if m:
            info["season"] = int(m.group(1))
            info["episode"] = int(m.group(2))
            info["media_type"] = "tv_show"
            break

    # Detect year
    year_match = re.search(r"(19|20)\d{2}", name)
    if year_match:
        info["year"] = int(year_match.group(0))

    # Split on dots, underscores, spaces
    parts = re.split(r"[._\s\-]+", name)

    # Title extraction
    if info["media_type"] == "tv_show":
        # Stop before SxxExx or 1x01
        stop_idx = 0
        for i, part in enumerate(parts):
            if re.match(r"[Ss]?\d{1,2}[Ee]?\d{1,2}", part):
                stop_idx = i
                break
        info["title"] = " ".join(parts[:stop_idx]).strip().title() or None
    else:
        # Probably a movie
        info["title"] = " ".join(p for p in parts if not re.match(r"(19|20)\d{2}", p)).title() or None
        info["media_type"] = "movie"

    # Extract episode title (if present after episode tag)
    ep_title_match = re.search(r"[Ss]?\d{1,2}[Ee]?\d{1,2}[._\-\s]+(.+)", name)
    if ep_title_match:
        ep_title_raw = ep_title_match.group(1)
        # remove common noise tokens
        ep_title_clean = re.sub(r"[._\-]+", " ", ep_title_raw).strip()
        if ep_title_clean and not re.search(r"(720p|1080p|x264|h264|bluray|webrip|hdrip|aac)", ep_title_clean, re.I):
            info["episode_title"] = ep_title_clean.title()

    return info


# -------------------------------
# ffprobe verification (optional)
# -------------------------------
def _detect_from_ffprobe(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Run ffprobe to extract basic metadata.
    Requires ffmpeg/ffprobe installed and in PATH.
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            file_path
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if proc.returncode != 0 or not proc.stdout:
            return None

        data = json.loads(proc.stdout)
        video_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "audio"), None)

        return {
            "duration": float(data.get("format", {}).get("duration", 0.0)),
            "video_codec": video_stream.get("codec_name") if video_stream else None,
            "audio_codec": audio_stream.get("codec_name") if audio_stream else None,
            "resolution": f"{video_stream.get('width')}x{video_stream.get('height')}" if video_stream else None,
            "format": data.get("format", {}).get("format_name")
        }
    except Exception as e:
        logger.warning(f"ffprobe failed for {file_path}: {e}")
        return None


# -------------------------------
# Unified detection interface
# -------------------------------
def detect_media_info(file_path: str, verify: bool = False) -> Dict[str, Any]:
    """
    Detect media information from filename and optionally verify with ffprobe.
    """
    base_info = _detect_from_filename(file_path)

    if verify:
        meta = _detect_from_ffprobe(file_path)
        if meta:
            base_info.update(meta)

    return base_info

