from datetime import datetime

import requests
import logging
from typing import Dict, Any, Optional, List


class TorrentMetadata:
    def __init__(self, config: Dict[str, Any]):
        self.session = requests.Session()
        self.timeout = 2  # short timeout for progress updates
        self.config = config.get('organizerr', {})
        self.logger = logging.getLogger(__name__)

        self.api = self.config.get('api')
        self.torrents_endpoint = 'torrents'
        self.file_operations_endpoint = 'api/file-operations'
        # backend API health-checks
        self.api_healthy = True
        self.last_health_check = None
        self.health_check_interval = 30  # seconds


    def check_api_health(self, force: bool = False) -> bool:
        now = datetime.utcnow().timestamp()

        # Avoid excessive health checks
        if (
                not force
                and self.last_health_check
                and now - self.last_health_check < self.health_check_interval
        ):
            return self.api_healthy

        self.last_health_check = now

        url = f"{self.api}/health/ready"

        try:
            response = self.session.get(
                url,
                timeout=3
            )

            response.raise_for_status()
            data = response.json()
            healthy = data.get("status") == "ready"

            # Recovery log
            if healthy and not self.api_healthy:
                self.logger.warning(
                    "Organizerr API recovered"
                )

            self.api_healthy = healthy
            return healthy

        except Exception as e:

            # Only log transition to unhealthy
            if self.api_healthy:
                self.logger.error(
                    f"Organizerr API became unhealthy: {e}"
                )
            self.api_healthy = False
            return False

    def fetch_torrent_metadata_by_hash(self, info_hash: str) -> Optional[Dict]:
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
        """Final operation logging (low-frequency, reliable)"""

        if not self.check_api_health():
            self.logger.warning(
                "Skipping file operation because API is unhealthy"
            )
            return False

        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            response = self.session.post(url, json=record, timeout=5)

            if response.status_code in (200, 201):
                self.logger.info(f"✅ File op success: {record.get('destination')}")
                return True
            else:
                self.logger.error(f"❌ Failed ({response.status_code}): {response.text}")

        except Exception as e:
            self.logger.warning(f"⚠️ File op error: {e}")

        return False


    def get_all_file_operation(self):
        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            self.logger.info(f"Trying API: {url}")
            response = requests.get(url, timeout=5)
            response.raise_for_status()

            if response.status_code in (200, 201):
                self.logger.info(f"✅ Request success: {response}")
                return response.json()
            else:
                self.logger.error(f"❌ Failed ({response.status_code}): {response.text}")

        except Exception as e:
            self.logger.warning(f"⚠️ Error: {e}")
        return None

    def send_progress_update(
            self,
            info_hash: str,
            file_hash: str,
            stage: str,
            progress: float,
            status: str = "processing",
            success: Optional[bool] = None,
            extra: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send progress updates (high-frequency, non-blocking safe)"""

        if not self.check_api_health():
            self.logger.debug(
                "Skipping progress update because API is unhealthy"
            )
            return False

        if not info_hash or not file_hash:
            return False

        payload = {
            "info_hash": info_hash,
            "file_hash": file_hash,
            "stage": stage,
            "progress": round(progress, 2),
            "status": status,
            "success": success,
            "timestamp": datetime.utcnow().isoformat()  # ✅ ADD
        }

        if extra:
            payload.update(extra)

        # stage/status normalization
        payload["stage"] = payload.get("stage") or "unknown"
        payload["status"] = payload.get("status") or "processing"

        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            self.logger.debug(f"Progress: {payload.get('stage')} {payload.get('progress')}%")
            # 🔥 FAST + NON-BLOCKING (important)
            response = self.session.post(
                url,
                json=payload,
                timeout=self.timeout
            )

            if response.status_code not in (200, 201):
                self.logger.debug(f"Progress update failed: {response.status_code}")

            if not self.api_healthy:
                self.logger.info(
                    "API communication restored"
                )

            self.api_healthy = True

            return True

        except Exception as e:
            # ❗ DO NOT log as error (too noisy)
            self.logger.debug(f"Progress update error: {e}")
            return False