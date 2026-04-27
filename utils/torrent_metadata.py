import requests
import logging
from typing import Dict, Any, Optional

class TorrentMetadata:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('organizerr', {})
        self.logger = logging.getLogger(__name__)
        self.torrent_api = self.config['api']
        self.api_endpoint = self.config['endpoint']

    def enrich_media_from_torrent(self, info_hash):
        torrent_api = "{}/{}/by_info_hash/{}".format(self.torrent_api, self.api_endpoint, info_hash)
        try:
            resp = requests.get(torrent_api, timeout=5)
            resp.raise_for_status()
            torrent_data = resp.json()
            # Use torrent_data to fill extra metadata in your media library
            return torrent_data
        except Exception as e:
            print(f"Failed to fetch torrent info: {e}")
            return None


    def fetch_all_torrent(self):
        torrent_api = "{}/{}".format(self.torrent_api, self.api_endpoint)
        try:
            resp = requests.get(torrent_api, timeout=5)
            resp.raise_for_status()
            torrent_data = resp.json()
            # Use torrent_data to fill extra metadata in your media library
            return torrent_data
        except Exception as e:
            print(f"Failed to fetch torrent info: {e}")
            return None

