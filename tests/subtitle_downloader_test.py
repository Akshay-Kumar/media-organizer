import yaml
import os
from pathlib import Path
import requests
import base64
import zlib
from opensubtitlescom import OpenSubtitles

# --- Load config ---
CONFIG_FILE = r"C:\Users\akki0\PycharmProjects\media-organizer\config\config.yaml"
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

opensub_conf = config['download']['opensubtitles']

API_KEY = opensub_conf['api_key']
USERNAME = opensub_conf['username']
PASSWORD = opensub_conf['password']
USER_AGENT = opensub_conf.get('user_agent', "SubDownloader 1.0")

# --- Initialize OpenSubtitles client ---
client = OpenSubtitles(USER_AGENT, API_KEY)
client.login(USERNAME, PASSWORD)


# --- Helper function to decompress subtitles ---
def decompress_subtitle(data: str) -> str:
    """Convert OpenSubtitles base64 + zlib compressed subtitles to string"""
    raw = zlib.decompress(base64.b64decode(data), 16 + zlib.MAX_WBITS)
    return raw.decode('utf-8', errors='ignore')


# --- Minimal wrapper function ---
def download_subtitle(query: str, media_type: str, season_number: int = None, episode_number: int = None,
                      year: int = None,
                      language: str = "en",
                      output_folder: str = "."):
    """
    Search and download the first subtitle for a given IMDb ID
    """
    if media_type in ["tv_show", "anime"]:
        response = client.search(type=media_type, query=query, season_number=season_number, episode_number=episode_number,
                                 languages=language)
    else:
        response = client.search(type=media_type, query=query, year=year, languages=language)

    subtitles = response.to_dict().get('data', [])

    if not subtitles:
        print(f"No subtitles found for IMDb ID {query}")
        return None

    # Pick first subtitle
    first_sub = subtitles[0]
    file_id = first_sub.file_id
    file_name = first_sub.file_name

    # Download subtitle
    subtitle_data = client.download(file_id)

    # Save to disk
    output_path = Path(output_folder) / (Path(file_name).stem + ".srt")
    with open(output_path, "wb") as srt:
        srt.write(subtitle_data)

    print(f"Saved subtitle: {output_path}")
    return output_path


# --- Example usage ---
if __name__ == "__main__":
    # IMDb ID for The Shawshank Redemption
    # query = "Alien Earth"
    # season_number = 1
    # episode_number = 8
    # download_subtitle(query=query, season_number=season_number, episode_number=episode_number, language="en")
    query = "Inuyasha the Movie 4 Fire on the Mystic Island"
    year = 2004
    media_type = "movie"
    download_subtitle(query=query, media_type=media_type,year=year, language="en")
