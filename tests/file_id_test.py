from core.file_identifier import MediaFileIdentifier
from utils.config_loader import ConfigLoader
from core.metadata_fetcher import MetadataFetcher
from core.media_parser import MediaParser


def main(config=None):
    config = ConfigLoader.load_config()
    config = ConfigLoader.validate_config(config)
    metadata_fetcher = MetadataFetcher(config)
    media_parser = MediaParser(config)
    identifier = MediaFileIdentifier(config, metadata_fetcher, media_parser)
    title1 = "Bleach"
    title2 = "Bleach: Thousand-Year Blood War"
    title3 = "Naruto Shippude"
    title4 = "Naruto: Shippūden"
    title5 = "The Day I Became a Shinigami"
    result = identifier.validate_series_name(title=title2)
    print(result)


if __name__ == "__main__":
    main()
