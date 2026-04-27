import requests
import logging
from typing import Dict, Any, Optional, List


class TorrentMetadata:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('organizerr', {})
        self.logger = logging.getLogger(__name__)

        self.api_win = self.config.get('api_win')
        self.api_rp4 = self.config.get('api_rp4')

        self.win_enabled = self.config.get('win_enabled', False)
        self.rp4_enabled = self.config.get('rp4_enabled', False)

        self.endpoint = self.config.get('endpoint', 'torrents')

    def _get_api_list(self) -> List[str]:
        apis = []

        if self.win_enabled and self.api_win:
            apis.append(self.api_win)

        if self.rp4_enabled and self.api_rp4:
            apis.append(self.api_rp4)

        return apis

    def enrich_media_from_torrent(self, info_hash: str) -> Optional[Dict]:
        for api in self._get_api_list():
            url = f"{api}/{self.endpoint}/by_info_hash/{info_hash}"

            try:
                self.logger.info(f"Trying API: {url}")
                resp = requests.get(url, timeout=5)
                resp.raise_for_status()

                return resp.json()

            except Exception as e:
                self.logger.warning(f"API failed: {url} → {e}")
                continue  # 🔥 try next API

        self.logger.error("All API calls failed")
        return None

    def fetch_all_torrent(self) -> Optional[Dict]:
        for api in self._get_api_list():
            url = f"{api}/{self.endpoint}"

            try:
                self.logger.info(f"Trying API: {url}")
                resp = requests.get(url, timeout=5)
                resp.raise_for_status()

                return resp.json()

            except Exception as e:
                self.logger.warning(f"API failed: {url} → {e}")
                continue

        self.logger.error("All API calls failed")
        return None