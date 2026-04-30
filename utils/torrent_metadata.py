import requests
import logging
from typing import Dict, Any, Optional, List


class TorrentMetadata:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('organizerr', {})
        self.logger = logging.getLogger(__name__)

        self.api = self.config.get('api')
        self.torrents_endpoint = 'torrents'
        self.file_operations_endpoint = 'api/file-operations'

    def enrich_media_from_torrent(self, info_hash: str) -> Optional[Dict]:
        url = f"{self.api}/{self.torrents_endpoint}/by_info_hash/{info_hash}"

        try:
            self.logger.info(f"Trying API: {url}")
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            self.logger.warning(f"API failed: {url} → {e}")
        return None

    def fetch_all_torrent(self) -> Optional[Dict]:
        url = f"{self.api}/{self.torrents_endpoint}"

        try:
            self.logger.info(f"Trying API: {url}")
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()

            return resp.json()

        except Exception as e:
            self.logger.warning(f"API failed: {url} → {e}")
        return None


    def send_file_operation(self, record: dict):
        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            self.logger.info(f"Trying API: {url}")
            response = requests.post(url, json=record, timeout=5)

            if response.status_code in (200, 201):
                self.logger.info(f"✅ Success: {record.get('destination')}")
                return True
            else:
                self.logger.error(f"❌ Failed ({response.status_code}): {response.text}")

        except Exception as e:
            self.logger.warning(f"⚠️ Error: {e}")
        return False