from plexapi.server import PlexServer
import json

PLEX_URL = "http://localhost:32400"
PLEX_TOKEN = "at-KCEsAxRxMXTf_fnHJ"

plex = PlexServer(PLEX_URL, PLEX_TOKEN)

show_title = "Wayward Pines"
show = plex.library.section("Transcoded TV Shows").get(show_title)

metadata = {
    "title": show.title,
    "key": show.key,
    "thumb": show.thumb,
    "tmdb_id": [g.id for g in show.guids if "tmdb" in g.id],
    "seasons": []
}

for season in show.seasons():
    season_data = {
        "title": season.title,
        "key": season.key,
        "thumb": season.thumb,
        "tmdb_id": [g.id for g in season.guids if "tmdb" in g.id],
        "episodes": []
    }
    for ep in season.episodes():
        season_data["episodes"].append({
            "title": ep.title,
            "index": ep.index,
            "thumb": ep.thumb,
            "tmdb_id": [g.id for g in ep.guids if "tmdb" in g.id]
        })
    metadata["seasons"].append(season_data)

print(json.dumps(metadata, indent=2))
