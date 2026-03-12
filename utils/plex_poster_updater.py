import argparse
import io
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import tmdbsimple as tmdb
from plexapi.server import PlexServer
from tqdm import tqdm

# --- Fix Windows console emoji issues ---
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8', errors='replace')

# === CONFIG ===
PLEX_URL = "http://localhost:32400"
PLEX_TOKEN = "at-KCEsAxRxMXTf_fnHJ"
TMDB_API_KEY = "488dbaa0bf23daf9ae9da9805775e3af"

MOVIE_LIBRARY = "Movies"
TV_SHOW_LIBRARY = "TV Shows"
TRANS_TV_SHOW_LIBRARY = "Transcoded TV Shows"
TRANS_MOVIE_LIBRARY = "Transcoded Movies"
ANIME_SHOW_LIBRARY = "Anime Shows"
ALL_LIBRARIES = [MOVIE_LIBRARY, TV_SHOW_LIBRARY, TRANS_MOVIE_LIBRARY, TRANS_TV_SHOW_LIBRARY, ANIME_SHOW_LIBRARY]
DELAY = 0.5
OUTPUT_DIR = Path("poster_update_logs")
OUTPUT_DIR.mkdir(exist_ok=True)

tmdb.API_KEY = TMDB_API_KEY
plex = PlexServer(PLEX_URL, PLEX_TOKEN)

# === LOGGING ===
log_file = OUTPUT_DIR / f"plex_poster_updater_{time.strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


# --- TMDb Search ---
def get_tmdb_poster_url(title, year=None, media_type="movie"):
    """Search TMDb by title + year and return poster URL."""
    try:
        search = tmdb.Search()
        if media_type == "movie":
            results = search.movie(query=title, year=year)
        else:
            results = search.tv(query=title)
        if results['results']:
            best_match = results['results'][0]
            poster_path = best_match.get("poster_path")
            if poster_path:
                return f"https://image.tmdb.org/t/p/original{poster_path}"
    except Exception as e:
        logging.warning(f"TMDb search failed for {title}: {e}")
    return None


# --- PLEX POSTER LOGIC ---
def select_best_plex_poster(item, title_hint=None):
    """Select the best Plex poster: always pick first poster."""
    posters = item.posters()
    if not posters:
        return None
    # Always return the first Plex poster
    return posters[0]


def set_default_if_possible(item, poster):
    """Safely set default poster for items that support it."""
    if poster is None:
        return False
    # Only Movie, Episode, TVShow, Season support setDefault
    if hasattr(item, "setPoster"):
        try:
            item.setPoster(poster)
            logging.info(f"🎯 Set Plex poster as default for {item.title}")
            return True
        except Exception as e:
            logging.warning(f"⚠ Failed to set Plex poster for {item.title}: {e}")
    return False


def upload_poster(item, poster_url, dry_run=False):
    """Upload poster from URL."""
    if dry_run:
        logging.info(f"🧪 Dry run: would upload poster for {item.title}")
        return True
    try:
        poster = item.uploadPoster(url=poster_url)
        logging.info(f"✅ Poster uploaded for {item.title}")
        return poster
    except Exception as e:
        logging.error(f"❌ Failed to upload poster for {item.title}: {e}")
    return None


def select_or_upload(item, title_hint=None, year=None, media_type="movie", dry_run=False):
    """Select Plex poster or upload from TMDb if none exist."""
    poster = select_best_plex_poster(item, title_hint=title_hint)
    if poster:
        set_default_if_possible(item, poster)
        return True

    # No Plex poster, fallback to TMDb
    poster_url = get_tmdb_poster_url(title_hint or item.title, year, media_type)
    if poster_url:
        poster = upload_poster(item, poster_url, dry_run=dry_run)
        set_default_if_possible(item, poster)
        return True

    logging.warning(f"⚠ No poster found for {item.title}")
    return False


# --- LIBRARY PROCESSING ---
def process_library(library_name, media_type="movie", dry_run=False):
    section = plex.library.section(library_name)
    items = section.all()
    results = []

    logging.info(f"🔍 Scanning {library_name} ({len(items)} items)...")

    for item in tqdm(items, desc=f"Processing {library_name}"):
        if media_type == "movie":
            # --- Handle Movies ---
            movie_uploaded = select_or_upload(item, year=getattr(item, 'year', None), media_type="movie",
                                              dry_run=dry_run)
            results.append({
                "Title": item.title,
                "Type": "movie",
                "Show Poster Missing": movie_uploaded,
                "Seasons Missing Poster": 0,
                "Episodes Missing Poster": 0
            })
            continue

        # --- Handle TV Shows ---
        show_uploaded = select_or_upload(item, year=getattr(item, 'year', None), media_type="tv", dry_run=dry_run)
        season_missing_count = 0
        episode_missing_count = 0

        for season in item.seasons():
            season_title_hint = f"{item.title} Season {season.index}"
            season_uploaded = select_or_upload(
                season,
                title_hint=season_title_hint,
                year=getattr(item, 'year', None),
                media_type="tv",
                dry_run=dry_run
            )
            if season_uploaded:
                season_missing_count += 1

            for ep in season.episodes():
                ep_title_hint = f"{item.title} S{season.index}E{ep.index} {ep.title}"
                ep_uploaded = select_or_upload(
                    ep,
                    title_hint=ep_title_hint,
                    year=getattr(item, 'year', None),
                    media_type="tv",
                    dry_run=dry_run
                )
                if ep_uploaded:
                    episode_missing_count += 1
                time.sleep(DELAY)

        results.append({
            "Title": item.title,
            "Type": "tv",
            "Show Poster Missing": show_uploaded,
            "Seasons Missing Poster": season_missing_count,
            "Episodes Missing Poster": episode_missing_count
        })

    return results


# --- MAIN ---
def main():
    parser = argparse.ArgumentParser(description="Plex Poster Updater: select Plex poster or fallback to TMDb")
    parser.add_argument("--movies-only", action="store_true")
    parser.add_argument("--shows-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.info("🚀 Starting Plex Poster Updater")
    all_results = []
    library_type = None

    if args.shows_only:
        all_results += process_library(TV_SHOW_LIBRARY, "tv", dry_run=args.dry_run)
        all_results += process_library(TRANS_TV_SHOW_LIBRARY, "tv", dry_run=args.dry_run)
        all_results += process_library(ANIME_SHOW_LIBRARY, "tv", dry_run=args.dry_run)
    elif args.movies_only:
        all_results += process_library(MOVIE_LIBRARY, "movie", dry_run=args.dry_run)
        all_results += process_library(TRANS_MOVIE_LIBRARY, "movie", dry_run=args.dry_run)
    else:
        for LIBRARY in ALL_LIBRARIES:
            if LIBRARY in [TRANS_TV_SHOW_LIBRARY, TV_SHOW_LIBRARY, ANIME_SHOW_LIBRARY]:
                library_type = "tv"
            elif LIBRARY in [MOVIE_LIBRARY, TRANS_MOVIE_LIBRARY]:
                library_type = "movie"
            else:
                library_type = "unknown"

            if library_type == "unknown":
                logging.warning(f"Invalid library_type: {library_type} detected for plex library: {LIBRARY}")
            else:
                all_results += process_library(LIBRARY, library_type, dry_run=args.dry_run)

    df = pd.DataFrame(all_results)
    csv_path = OUTPUT_DIR / f"poster_update_summary_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    summary = {
        "Show Posters Missing": df["Show Poster Missing"].sum(),
        "Seasons Missing Poster": df["Seasons Missing Poster"].sum(),
        "Episodes Missing Poster": df["Episodes Missing Poster"].sum()
    }
    logging.info(f"📊 Summary: {summary}")
    logging.info(f"📁 Log saved: {log_file}")
    logging.info(f"📈 Summary CSV saved: {csv_path}")
    logging.info("🎉 Done!")


if __name__ == "__main__":
    main()
