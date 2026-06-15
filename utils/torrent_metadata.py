from datetime import datetime
import json
import requests
import logging
from typing import Dict, Any, Optional, List


class TorrentMetadata:
    def __init__(self, config: Dict[str, Any]):
        self.session = requests.Session()
        self.config = config.get('organizerr', {})
        self.logger = logging.getLogger(__name__)

        self.api = self.config.get('api')
        self.timeout = self.config.get('max_timeout', 2)
        self.retry_count = self.config.get('retry_count', 3)
        self.torrents_endpoint = 'torrents'
        self.file_operations_endpoint = 'api/file-operations'
        self.processing_reports_endpoint = 'api/processing-reports'
        # backend API health checks
        self.api_healthy = True
        self.last_health_check = None
        self.health_check_interval = 30  # seconds
        self._stage_progress = {}

    def _json_safe(self, obj):
        try:
            return json.loads(
                json.dumps(
                    obj,
                    default=str
                )
            )
        except Exception:
            return {}

    def _post_with_retry(
            self,
            url: str,
            payload: dict,
            timeout: int,
            retries: int = 3
    ) -> bool:

        import time

        for attempt in range(retries):
            try:
                response = self.session.post(
                    url,
                    json=payload,
                    timeout=timeout
                )
                response.raise_for_status()
                return True

            except requests.HTTPError as e:
                status_code = (
                    e.response.status_code
                    if e.response
                    else None
                )

                #
                # Don't retry permanent errors
                #
                if status_code and 400 <= status_code < 500:
                    raise

            except Exception:
                pass

            if attempt < retries - 1:
                wait_time = 2 ** attempt

                self.logger.warning(
                    f"Retrying request in "
                    f"{wait_time}s "
                    f"(attempt {attempt + 2}/{retries})"
                )

                time.sleep(wait_time)

        return False

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
                timeout=self.timeout
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
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            self.logger.warning(f"API failed: {url} → {e}")
        return None

    def fetch_all_torrent(self) -> Optional[Dict]:
        url = f"{self.api}/{self.torrents_endpoint}"

        try:
            self.logger.info(f"Trying API: {url}")
            resp = requests.get(url, timeout=self.timeout)
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
            response = self.session.post(url, json=record, timeout=self.timeout)

            if response.status_code in (200, 201):
                self.logger.info(f"✅ File op success: {record.get('destination')}")
                return True
            else:
                self.logger.error(f"❌ Failed ({response.status_code}): {response.text}")

        except Exception as e:
            self.logger.warning(f"⚠️ File op error: {e}")

        return False

    def send_processing_report(
            self,
            report: dict
    ) -> bool:

        if not self.check_api_health():
            self.logger.warning(
                "Skipping processing report because API is unhealthy"
            )
            return False

        file_info = report.get("file_info", {})
        file_hash = file_info.get("hash")
        info_hash = report.get("info_hash")

        if not info_hash or not file_hash:
            return False

        safe_report = json.loads(
            json.dumps(
                report,
                default=str
            )
        )

        payload = {
            "info_hash": info_hash,
            "file_hash": file_hash,

            "media_type": report.get("media_type"),

            "source_path": report.get(
                "original_path"
            ),

            "destination_path": (
                report.get("move_result", {})
                .get("destination")
            ),

            "success": report.get(
                "success",
                False
            ),

            "processing_time": report.get(
                "processing_time"
            ),

            "report": safe_report
        }

        url = (
            f"{self.api}/"
            f"{self.processing_reports_endpoint}"
        )

        try:

            self.logger.info(
                f"Sending processing report "
                f"for {payload.get('file_hash')}"
            )

            success = self._post_with_retry(
                url,
                payload,
                timeout=self.timeout,
                retries=self.retry_count
            )

            if success:
                self.logger.info(
                    f"Processing report stored: "
                    f"{payload.get('file_hash')}"
                )
                return True

            self.logger.error(
                f"Processing report failed "
                f"after retries"
            )
            return False

        except Exception as e:
            self.logger.exception(
                f"Processing report error: {e}"
            )

        return False


    def get_all_file_operation(self):
        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            self.logger.info(f"Trying API: {url}")
            response = requests.get(url, timeout=self.timeout)
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

        key = f"{info_hash}:{file_hash}:{stage}"
        last = self._stage_progress.get(key)
        if last is not None and progress < last:
            return False

        self._stage_progress[key] = progress

        if status in ("completed", "failed", "skipped"):
            self._stage_progress.pop(key, None)

        if not self.check_api_health():
            self.logger.warning(
                "API marked unhealthy, "
                "trying progress update anyway"
            )

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
        payload["status"] = (
                payload.get("status")
                or "processing"
        )

        valid_statuses = {
            "processing",
            "completed",
            "failed",
            "skipped"
        }

        if payload["status"] not in valid_statuses:
            payload["status"] = "processing"

        url = f"{self.api}/{self.file_operations_endpoint}"

        try:
            self.logger.debug(f"Progress: {payload.get('stage')} {payload.get('progress')}%")
            if status == "skipped":
                self.logger.info(
                    f"Skipped file: {file_hash}"
                )

            # 🔥 POST with retry logic to avoid incomplete progress updates
            success = self._post_with_retry(
                url,
                payload,
                timeout=self.timeout,
                retries=self.retry_count
            )

            if success:
                self.api_healthy = True

                return True

            self.logger.warning(
                f"Failed progress update "
                f"after retries: "
                f"{stage}"
            )

            return False

        except Exception as e:
            # ❗ DO NOT log as error (too noisy)
            self.logger.debug(f"Progress update error: {e}")
            return False