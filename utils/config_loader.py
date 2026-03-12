import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import os


class ConfigLoader:
    @staticmethod
    def load_config(config_path: str = 'config/config.yaml') -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)

        if not config_file.exists():
            logging.warning(f"Config file not found: {config_path}")
            return ConfigLoader.get_default_config()

        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}

            # Merge with default config to ensure all required keys exist
            default_config = ConfigLoader.get_default_config()
            merged_config = ConfigLoader.deep_merge(default_config, config)

            logging.info(f"Configuration loaded from {config_path}")
            return merged_config

        except Exception as e:
            logging.error(f"Error loading config file {config_path}: {e}")
            return ConfigLoader.get_default_config()

    @staticmethod
    def get_default_config() -> Dict[str, Any]:
        """Return default configuration"""
        return {
            'api_keys': {
                'tmdb': os.getenv('TMDB_API_KEY', ''),
                'tvdb': os.getenv('TVDB_API_KEY', ''),
                'imdb': os.getenv('IMDB_API_KEY', ''),
                'anilist': os.getenv('ANILIST_CLIENT_ID', ''),
                'musicbrainz': os.getenv('MUSICBRAINZ_USER_AGENT', 'media-organizer/1.0')
            },
            'patterns': {
                'movie': [
                    "{title} ({year}){extension}",
                    "{title} ({year}) - {quality}{extension}"
                ],
                'tv_show': [
                    "{title} - S{season:02d}E{episode:02d} - {episode_title}{extension}",
                    "{title} - {season}x{episode:02d} - {episode_title}{extension}"
                ],
                'anime': [
                    "{title} - S{season:02d}E{episode:02d} - {episode_title}{extension}",
                    "{title} - {episode:03d} - {episode_title}{extension}"
                ],
                'music': [
                    "{track:02d} - {title}{extension}",
                    "{disc:02d}-{track:02d} - {title}{extension}"
                ]
            },
            'plex_paths': {
                'movies': "/media/plex/Movies",
                'tv_shows': "/media/plex/TV Shows",
                'anime': "/media/plex/Anime",
                'music': "/media/plex/Music"
            },
            'download': {
                'artwork': True,
                'subtitles': True,
                'subtitle_languages': ["en", "es", "fr"],
                'artwork_sizes': {
                    'poster': "w500",
                    'backdrop': "w1280",
                    'thumb': "w300"
                }
            },
            'processing': {
                'max_retries': 3,
                'timeout': 30,
                'concurrent_downloads': 5,
                'max_file_size_mb': 102400  # 10GB
            },
            'logging': {
                'level': 'INFO',
                'file': 'logs/media_organizer.log',
                'max_size_mb': 100,
                'backup_count': 5
            },
            'api_timeouts': {
                'tmdb': 30,
                'tvdb': 30,
                'musicbrainz': 30,
                'anilist': 30,
                'subtitle': 60
            },
            'retry_policy': {
                'max_retries': 3,
                'backoff_factor': 1.0
            }
        }

    @staticmethod
    def deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge two dictionaries"""
        result = base.copy()

        for key, value in update.items():
            if (key in result and isinstance(result[key], dict)
                    and isinstance(value, dict)):
                result[key] = ConfigLoader.deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    @staticmethod
    def save_config(config: Dict[str, Any], config_path: str) -> bool:
        """Save configuration to file"""
        try:
            config_file = Path(config_path)
            config_file.parent.mkdir(parents=True, exist_ok=True)

            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)

            logging.info(f"Configuration saved to {config_path}")
            return True

        except Exception as e:
            logging.error(f"Error saving config to {config_path}: {e}")
            return False

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced configuration validation"""
        validated = config.copy()

        # Validate API keys
        for key in ['tmdb', 'tvdb']:
            if not validated['api_keys'].get(key):
                logging.warning(f"Missing API key for {key}. Some features may not work.")

        # Validate and create paths
        for media_type, path in validated['plex_paths'].items():
            path_obj = Path(path)
            if not path_obj.exists():
                logging.warning(f"Plex path for {media_type} does not exist: {path}")
                try:
                    path_obj.mkdir(parents=True, exist_ok=True)
                    logging.info(f"Created directory: {path}")
                except PermissionError:
                    logging.error(f"Permission denied creating directory: {path}")
                    validated['plex_paths'][media_type] = Path.cwd() / media_type

        # Validate file patterns
        for media_type, patterns in validated['patterns'].items():
            if not patterns:
                logging.warning(f"No patterns defined for {media_type}")
                validated['patterns'][media_type] = [f"/{media_type}/{{title}}{{extension}}"]

        # Validate download settings
        if not validated['download'].get('subtitle_languages'):
            validated['download']['subtitle_languages'] = ['en']

        return validated

    @staticmethod
    def load_json_config(json_path: str) -> Optional[Dict[str, Any]]:
        """Load configuration from JSON file (alternative format)"""
        config_file = Path(json_path)

        if not config_file.exists():
            return None

        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading JSON config {json_path}: {e}")
            return None