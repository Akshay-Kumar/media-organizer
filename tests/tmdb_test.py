from core.metadata_fetcher import MetadataFetcher
from utils.config_loader import ConfigLoader


def main():
    media_info = {
        'series': 'InuYasha',
        'season': 4,
        'episode': 2,
        'filename': 'InuYasha - S04E02.mkv',
        'media_type': 'anime'
    }
    config_path = '../config/config.yaml'
    config = ConfigLoader.load_config(config_path)
    metadata_fetcher = MetadataFetcher(config)
    metadata_fetcher._setup_tmdb()
    # result = metadata_fetcher._fetch_tv_metadata(media_info)
    result = metadata_fetcher.fetch_metadata(media_info)
    print(result)


if __name__ == "__main__":
    exit(main())
