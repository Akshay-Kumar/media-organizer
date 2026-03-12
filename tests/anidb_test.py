from utils.config_loader import ConfigLoader
from core.anidb_client import AniDBClient


def main():
    media_info = {
        'series': 'InuYasha',
        'season': 4,
        'episode': 2,
        'filename': 'InuYasha - S04E02.mkv',
        'media_type': 'anime'
    }
    anime_title = media_info.get("series")
    config_path = '../config/config.yaml'
    config = ConfigLoader.load_config(config_path)
    # anidb_client = AniDBClient(config)
    # result = metadata_fetcher._fetch_tv_metadata(media_info)
    # result = anidb_client.search_anime(f"{anime_title}")
    # print(result)


if __name__ == "__main__":
    exit(main())
