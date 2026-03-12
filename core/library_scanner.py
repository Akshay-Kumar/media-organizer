import logging
import requests
import time
from typing import Dict, Any, List, Optional
from pathlib import Path


class LibraryScanner:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('library_scan', {})
        self.logger = logging.getLogger(__name__)
        self.audit_logger = logging.getLogger('audit')
        self._plex_libraries_cache = None
        self._plex_libraries_cache_time = 0

    def _get_plex_libraries(self) -> Optional[Dict[str, str]]:
        """Get all Plex libraries with their names and IDs"""
        # Cache libraries for 5 minutes to avoid repeated API calls
        current_time = time.time()
        if (self._plex_libraries_cache and
                current_time - self._plex_libraries_cache_time < 300):
            return self._plex_libraries_cache

        plex_config = self.config.get('plex', {})
        if not plex_config.get('enabled', False):
            return None

        try:
            base_url = plex_config['base_url'].rstrip('/')
            token = plex_config['token']

            libraries_url = f"{base_url}/library/sections?X-Plex-Token={token}"
            response = requests.get(libraries_url, timeout=30)

            if response.status_code == 200:
                from xml.etree import ElementTree as ET
                root = ET.fromstring(response.content)

                libraries = {}
                for directory in root.findall('Directory'):
                    library_name = directory.get('title')
                    library_id = directory.get('key')
                    libraries[library_name] = library_id

                self._plex_libraries_cache = libraries
                self._plex_libraries_cache_time = current_time
                return libraries
            else:
                self.logger.error(f"Failed to get Plex libraries: {response.status_code}")
                return None

        except Exception as e:
            self.logger.error(f"Error fetching Plex libraries: {e}")
            return None

    def _get_libraries_for_media_type(self, media_type: str, server_type: str = 'plex') -> Optional[list]:
        """Get the library ID for a specific media type"""
        found_ids = []
        if server_type == 'plex':
            library_mapping = self.config.get('plex', {}).get('library_mapping', {})
            target_library_names = library_mapping.get(media_type)

            if not target_library_names:
                return []

            # Support both string and list types in config
            if isinstance(target_library_names, str):
                target_library_names = [target_library_names]

            libraries = self._get_plex_libraries()

            if libraries:
                for name in target_library_names:
                    if name in libraries:
                        library = {
                            "id": libraries[name],
                            "name": name
                        }
                        found_ids.append(library)
                    else:
                        self.logger.warning(f"Plex library '{name}' not found for media type '{media_type}'")

        elif server_type == 'emby':
            # For Emby, we'd need to implement similar logic
            # This is a placeholder - Emby implementation would be similar
            return []

        return found_ids

    def trigger_plex_scan(self, media_type: Optional[str] = None) -> list:
        """Trigger Plex library scan for specific media type or all libraries"""
        plex_scan_results = []
        if not self.config.get('plex', {}).get('enabled', False):
            return [{"Plex scan not enabled in config": False}]

        try:
            plex_config = self.config['plex']
            base_url = plex_config['base_url'].rstrip('/')
            token = plex_config['token']

            # If media_type is specified, scan only that library
            if media_type:
                libraries = self._get_libraries_for_media_type(media_type, 'plex')
                if libraries and len(libraries) > 0:
                    for library in libraries:
                        if isinstance(library, dict):
                            library_id = library.get('id')
                            library_name = library.get('name')
                            scan_url = f"{base_url}/library/sections/{library_id}/refresh?X-Plex-Token={token}"
                            response = requests.get(scan_url, timeout=60)

                            if response.status_code == 200:
                                self.logger.info(f"Plex scan triggered for {library_name} library")
                                self.audit_logger.info(f"Plex {library_name} library scanned")
                                plex_scan_results.append({
                                    library_name: True
                                })
                            else:
                                self.logger.error(f"Plex scan failed for {library_name}: {response.status_code}")
                                plex_scan_results.append({
                                    f"Plex scan failed for {library_name}: {response.status_code}": False
                                })
                else:
                    # Media type not mapped or library not found
                    if plex_config.get('scan_all_if_unmapped', False):
                        self.logger.info(f"Media type '{media_type}' not mapped, scanning all libraries")
                        return self.trigger_plex_scan()  # Fallback to scan all
                    else:
                        self.logger.warning(f"Media type '{media_type}' not mapped to any library")
                        return [{f"Media type '{media_type}' not mapped to any library": False}]
            else:
                # Trigger full plex scan if no media type specified or fallback
                scan_url = f"{base_url}/library/sections/all/refresh?X-Plex-Token={token}"
                response = requests.get(scan_url, timeout=120)  # Longer timeout for full scan

                if response.status_code == 200:
                    self.logger.info("Plex full library scan triggered successfully")
                    self.audit_logger.info("All Plex libraries scanned")
                    return [{"All Plex libraries scanned": True}]
                else:
                    self.logger.error(f"Plex full scan failed: {response.status_code}")
                    return [{f"Plex full scan failed: {response.status_code}": False}]

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Plex scan request failed: {e}")
            return [{f"Plex scan request failed: {e}": False}]
        except Exception as e:
            self.logger.error(f"Unexpected error during Plex scan: {e}")
            return [{f"Unexpected error during Plex scan: {e}": False}]

        return plex_scan_results

    def trigger_emby_scan(self, media_type: Optional[str] = None) -> list:
        """Trigger Emby library scan for specific media type"""
        emby_scan_results = []
        if not self.config.get('emby', {}).get('enabled', False):
            return [{"Emby scan not enabled in config": False}]

        try:
            emby_config = self.config['emby']
            base_url = emby_config['base_url'].rstrip('/')
            api_key = emby_config['api_key']

            # Emby doesn't have a direct per-library scan endpoint in the same way
            # We'll trigger a full scan but log the intended media type
            scan_url = f"{base_url}/Library/Refresh?api_key={api_key}"
            response = requests.post(scan_url, timeout=60)

            if response.status_code in [200, 204]:
                if media_type:
                    self.logger.info(f"Emby scan triggered (media type: {media_type})")
                    self.audit_logger.info(f"Emby libraries scanned for {media_type}")
                    emby_scan_results.append({
                        f"Emby scan triggered (media type: {media_type})": True
                    })
                else:
                    self.logger.info("Emby full library scan triggered")
                    self.audit_logger.info("All Emby libraries scanned")
                    emby_scan_results.append({
                        "Emby full library scan triggered": True
                    })
            else:
                self.logger.error(f"Emby scan failed: {response.status_code}")
                return [{f"Emby scan failed: {response.status_code}": False}]

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Emby scan request failed: {e}")
            return [{f"Emby scan request failed: {e}": False}]
        except Exception as e:
            self.logger.error(f"Unexpected error during Emby scan: {e}")
            return [{f"Unexpected error during Emby scan: {e}": False}]

        return emby_scan_results

    # In LibraryScanner class, modify the scan methods to return duration
    def scan_libraries(self, media_type: Optional[str] = None) -> Dict[str, Any]:
        """Scan libraries based on media type and return results with duration"""
        if not self.config.get('enabled', True):
            return {'plex': False, 'emby': False, 'duration': 0}

        start_time = time.time()

        # Add delay if configured
        delay = self.config.get('scan_delay_seconds', 5)
        if delay > 0:
            self.logger.debug(f"Waiting {delay} seconds before library scan...")
            time.sleep(delay)

        results = {
            'plex': self.trigger_plex_scan(media_type),
            'emby': self.trigger_emby_scan(media_type),
            'duration': time.time() - start_time
        }

        return results

    def get_available_libraries(self) -> Dict[str, List[str]]:
        """Get list of available libraries for debugging"""
        libraries_info = {
            'plex': [],
            'emby': []  # Placeholder for Emby
        }

        # Get Plex libraries
        plex_libs = self._get_plex_libraries()
        if plex_libs:
            libraries_info['plex'] = list(plex_libs.keys())

        return libraries_info
