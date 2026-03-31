from pathlib import Path
import requests
import logging
from typing import Dict, Any
import shutil
from datetime import datetime
import psutil
import socket
import time
import os
from xml.etree import ElementTree as ET

from opensubtitlescom import OpenSubtitles


class HealthChecker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def check_apis(self) -> Dict[str, Any]:
        """Check connectivity to all APIs with detailed status"""
        results = {}

        # TMDB check
        try:
            start_time = time.time()
            response = requests.get(
                "https://api.themoviedb.org/3/configuration",
                params={'api_key': self.config['api_keys']['tmdb']},
                timeout=10
            )
            response_time = time.time() - start_time
            results['tmdb'] = {
                'status': response.status_code == 200,
                'response_time': round(response_time, 3),
                'status_code': response.status_code
            }
        except Exception as e:
            results['tmdb'] = {
                'status': False,
                'error': str(e),
                'response_time': None
            }

        # TVDB: 401 is acceptable here because root isn't an authenticated resource
        try:
            start_time = time.time()
            response = requests.get(
                "https://api4.thetvdb.com/v4",
                timeout=10
            )
            response_time = time.time() - start_time
            # TVDB returns 401 for unauthorized access, which means API is up
            results['tvdb'] = {
                'status': response.status_code in [200, 401],
                'response_time': round(response_time, 3),
                'status_code': response.status_code
            }
        except Exception as e:
            results['tvdb'] = {
                'status': False,
                'error': str(e),
                'response_time': None
            }

        # AniList: must be POST to GraphQL endpoint
        try:
            start_time = time.time()
            response = requests.post(
                "https://graphql.anilist.co",
                json={"query": "query { Media(id: 1) { id } }"},
                timeout=10,
            )
            response_time = time.time() - start_time
            # Viewer may require auth; if you want unauthenticated health, use a public query instead
            results['anilist'] = {
                'status': response.status_code in (200, 400),  # endpoint reachable
                'response_time': round(response_time, 3),
                'status_code': response.status_code
            }
        except Exception as e:
            results['anilist'] = {
                'status': False,
                'error': str(e),
                'response_time': None
            }

        # MusicBrainz: valid search endpoint + required User-Agent
        try:
            start_time = time.time()
            response = requests.get(
                "https://musicbrainz.org/ws/2/artist",
                params={"query": "artist:Beatles", "fmt": "json", "limit": 1},
                headers={"User-Agent": self.config.get('api_keys', {}).get('musicbrainz')},
                timeout=10,
            )
            response_time = time.time() - start_time
            results['musicbrainz'] = {
                'status': response.status_code == 200,
                'response_time': round(response_time, 3),
                'status_code': response.status_code
            }
        except Exception as e:
            results['musicbrainz'] = {
                'status': False,
                'error': str(e),
                'response_time': None
            }

        # OpenSubtitles: use a stricter User-Agent format
        opensub_conf = self.config.get('download', {}).get('opensubtitles', {})
        if 'opensubtitles' in self.config.get('download', {}):
            try:
                user_agent = opensub_conf.get('user_agent', 'media-organizer 1.0')
                client = OpenSubtitles(user_agent, opensub_conf['api_key'])
                resp = client.login(opensub_conf['username'], opensub_conf['password'])
                start_time = time.time()

                login_ok = bool(resp and resp.get("token"))

                response_time = time.time() - start_time
                results['opensubtitles'] = {
                    'status': login_ok,
                    'response_time': round(response_time, 3),
                    'status_code': 200 if login_ok else None
                }
            except Exception as e:
                results['opensubtitles'] = {
                    'status': False,
                    'error': str(e),
                    'response_time': None
                }

        return results

    def check_plex_connectivity(self) -> Dict[str, Any]:
        """Check Plex server connectivity and library mapping"""
        plex_config = self.config.get('library_scan', {}).get('plex', {})
        if not plex_config.get('enabled', False):
            return {'status': True, 'configured': False, 'message': 'Plex scanning not enabled'}

        try:
            base_url = plex_config['base_url'].rstrip('/')
            token = plex_config['token']

            # Test basic connectivity
            test_url = f"{base_url}/?X-Plex-Token={token}"
            response = requests.get(test_url, timeout=10)
            if response.status_code != 200:
                return {
                    'status': False,
                    'configured': True,
                    'error': f'Plex server unreachable: HTTP {response.status_code}',
                    'connectivity': False
                }

            # Test library access and validate mappings
            libraries_url = f"{base_url}/library/sections?X-Plex-Token={token}"
            lib_response = requests.get(libraries_url, timeout=15)

            if lib_response.status_code != 200:
                return {
                    'status': False,
                    'configured': True,
                    'error': f'Cannot access Plex libraries: HTTP {lib_response.status_code}',
                    'connectivity': True,
                    'library_access': False
                }

            # Parse library information
            root = ET.fromstring(lib_response.content)
            available_libraries = {}
            for directory in root.findall('Directory'):
                library_name = directory.get('title')
                library_id = directory.get('key')
                library_type = directory.get('type', 'unknown')
                available_libraries[library_name] = {
                    'id': library_id,
                    'type': library_type
                }

            # Validate library mappings
            library_mapping = plex_config.get('library_mapping', {})
            mapping_validation = {}
            all_mappings_valid = True

            for media_type, configured_library in library_mapping.items():
                # configured library can be string or list[str]
                candidates = configured_library if isinstance(configured_library, list) else [configured_library]

                matched_name = next((name for name in candidates if name in available_libraries), None)
                library_exists = matched_name is not None

                mapping_validation[media_type] = {
                    'configured_library': configured_library,
                    'matched_library': matched_name,
                    'exists': library_exists,
                    'library_id': available_libraries[matched_name]['id'] if library_exists else None,
                    'library_type': available_libraries[matched_name]['type'] if library_exists else None,
                    'candidates': candidates,
                    'missing': [name for name in candidates if name not in available_libraries],
                }

                if not library_exists:
                    all_mappings_valid = False

            return {
                'status': all_mappings_valid,
                'configured': True,
                'connectivity': True,
                'library_access': True,
                'mappings_valid': all_mappings_valid,
                'available_libraries': list(available_libraries.keys()),
                'mapping_validation': mapping_validation,
                'base_url': base_url,
                'server_accessible': True
            }

        except requests.exceptions.RequestException as e:
            return {
                'status': False,
                'configured': True,
                'error': f'Request error: {str(e)}',
                'connectivity': False
            }
        except Exception as e:
            return {
                'status': False,
                'configured': True,
                'error': f'Unexpected error: {str(e)}',
                'connectivity': False
            }

    def check_emby_connectivity(self) -> Dict[str, Any]:
        """Check Emby server connectivity"""
        emby_config = self.config.get('library_scan', {}).get('emby', {})
        if not emby_config.get('enabled', False):
            return {'status': True, 'configured': False, 'message': 'Emby scanning not enabled'}

        try:
            base_url = emby_config['base_url'].rstrip('/')
            api_key = emby_config['api_key']

            test_url = f"{base_url}/System/Info?api_key={api_key}"
            response = requests.get(test_url, timeout=10)

            if response.status_code == 200:
                return {
                    'status': True,
                    'configured': True,
                    'connectivity': True,
                    'server_info': response.json().get('ServerName', 'Unknown'),
                    'version': response.json().get('Version', 'Unknown')
                }
            else:
                return {
                    'status': False,
                    'configured': True,
                    'error': f'Emby server unreachable: HTTP {response.status_code}',
                    'connectivity': False
                }

        except requests.exceptions.RequestException as e:
            return {
                'status': False,
                'configured': True,
                'error': f'Request error: {str(e)}',
                'connectivity': False
            }
        except Exception as e:
            return {
                'status': False,
                'configured': True,
                'error': f'Unexpected error: {str(e)}',
                'connectivity': False
            }

    def check_media_servers(self) -> Dict[str, Any]:
        """Check both Plex and Emby connectivity"""
        return {
            'plex': self.check_plex_connectivity(),
            'emby': self.check_emby_connectivity()
        }

    def check_disk_space(self, path: str = '.') -> Dict[str, Any]:
        """Check available disk space with detailed information"""
        try:
            usage = shutil.disk_usage(path)
            return {
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent_free': round((usage.free / usage.total) * 100, 2),
                'percent_used': round((usage.used / usage.total) * 100, 2),
                'total_gb': round(usage.total / (1024 ** 3), 2),
                'used_gb': round(usage.used / (1024 ** 3), 2),
                'free_gb': round(usage.free / (1024 ** 3), 2),
                'path': path,
                'status': (usage.free / usage.total) > 0.1  # Warn if less than 10% free
            }
        except Exception as e:
            return {'error': str(e), 'status': False}

    def check_system_resources(self) -> Dict[str, Any]:
        """Check system CPU and memory usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)

            # Memory usage
            memory = psutil.virtual_memory()

            # Disk I/O
            disk_io = psutil.disk_io_counters()

            # Network
            net_io = psutil.net_io_counters()

            return {
                'cpu': {
                    'percent': cpu_percent,
                    'cores': psutil.cpu_count(),
                    'status': cpu_percent < 90  # Warn if over 90%
                },
                'memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'percent': memory.percent,
                    'total_gb': round(memory.total / (1024 ** 3), 2),
                    'available_gb': round(memory.available / (1024 ** 3), 2),
                    'used_gb': round(memory.used / (1024 ** 3), 2),
                    'status': memory.percent < 90  # Warn if over 90%
                },
                'disk_io': {
                    'read_bytes': disk_io.read_bytes if disk_io else 0,
                    'write_bytes': disk_io.write_bytes if disk_io else 0,
                    'read_mb': round(disk_io.read_bytes / (1024 ** 2), 2) if disk_io else 0,
                    'write_mb': round(disk_io.write_bytes / (1024 ** 2), 2) if disk_io else 0
                } if disk_io else {},
                'network': {
                    'bytes_sent': net_io.bytes_sent,
                    'bytes_recv': net_io.bytes_recv,
                    'mb_sent': round(net_io.bytes_sent / (1024 ** 2), 2),
                    'mb_recv': round(net_io.bytes_recv / (1024 ** 2), 2)
                } if net_io else {}
            }
        except Exception as e:
            return {'error': str(e)}

    def check_network_connectivity(self) -> Dict[str, Any]:
        """Check basic network connectivity"""
        results = {}

        # Check DNS resolution
        try:
            socket.gethostbyname('google.com')
            results['dns'] = True
        except:
            results['dns'] = False

        # Check internet connectivity
        try:
            response = requests.get('https://httpbin.org/get', timeout=10)
            results['internet'] = response.status_code == 200
        except:
            results['internet'] = False

        # Check if ports are accessible (common media server ports)
        ports_to_check = [32400, 8080, 8096]  # Plex, HTTP alternative, Jellyfin
        results['ports'] = {}

        for port in ports_to_check:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex(('127.0.0.1', port))
                results['ports'][port] = result == 0
                sock.close()
            except:
                results['ports'][port] = False

        return results

    def check_required_directories(self) -> Dict[str, Any]:
        """Check if required directories exist and are accessible"""
        results = {}
        required_dirs = [
            '.',  # Current directory
            self.config.get('backup_dir', 'backups'),
            'logs',
            'cache'
        ]

        # Add Plex library paths
        plex_paths = self.config.get('plex_paths', {})
        for media_type, path in plex_paths.items():
            if path:  # Only add if path is not empty
                required_dirs.append(path)

        for directory in required_dirs:
            try:
                path = Path(directory)
                stats = {
                    'exists': path.exists(),
                    'is_dir': path.is_dir() if path.exists() else False,
                    'readable': os.access(path, os.R_OK) if path.exists() else False,
                    'writable': os.access(path, os.W_OK) if path.exists() else False
                }
                if path.exists() and path.is_dir():
                    stats['file_count'] = len(list(path.glob('*')))
                results[directory] = stats
            except Exception as e:
                results[directory] = {'error': str(e), 'exists': False}

        return results

    def check_file_handles(self) -> Dict[str, Any]:
        """Check file handle limits and usage"""
        try:
            if hasattr(psutil, 'RLIMIT_NOFILE'):
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                return {
                    'soft_limit': soft,
                    'hard_limit': hard,
                    'status': soft >= 1024  # Warn if less than 1024
                }
            return {'error': 'RLIMIT_NOFILE not available on this system'}
        except Exception as e:
            return {'error': str(e)}

    def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check"""
        start_time = time.time()

        health_data = {
            'apis': self.check_apis(),
            'media_servers': self.check_media_servers(),
            'disk_space': self.check_disk_space('.'),
            'system_resources': self.check_system_resources(),
            'network': self.check_network_connectivity(),
            'directories': self.check_required_directories(),
            'file_handles': self.check_file_handles(),
            'timestamp': datetime.now().isoformat(),
            'check_duration': 0
        }

        health_data['check_duration'] = round(time.time() - start_time, 3)

        # Calculate overall status
        all_ok = True
        issues = []

        # Check API status
        for api_name, api_status in health_data['apis'].items():
            if not api_status.get('status', False):
                all_ok = False
                issues.append(f"API {api_name} unavailable")

        # Check media servers
        for server_name, server_status in health_data['media_servers'].items():
            if server_status.get('configured', False) and not server_status.get('status', True):
                all_ok = False
                issues.append(f"{server_name.capitalize()} server issue: {server_status.get('error', 'Unknown error')}")

        # Check disk space
        if not health_data['disk_space'].get('status', True):
            all_ok = False
            issues.append("Low disk space")

        # Check system resources
        if not health_data['system_resources'].get('cpu', {}).get('status', True):
            all_ok = False
            issues.append("High CPU usage")
        if not health_data['system_resources'].get('memory', {}).get('status', True):
            all_ok = False
            issues.append("High memory usage")

        health_data['overall_status'] = all_ok
        health_data['issues'] = issues
        health_data['issue_count'] = len(issues)

        return health_data

    def get_health_summary(self) -> str:
        """Get a human-readable health summary"""
        health = self.run_health_check()

        summary = []
        summary.append("=== SYSTEM HEALTH SUMMARY ===")
        summary.append(f"Overall Status: {'✅ HEALTHY' if health['overall_status'] else '❌ UNHEALTHY'}")
        summary.append(f"Check Duration: {health['check_duration']}s")

        if health['issues']:
            summary.append("\n⚠️  ISSUES:")
            for issue in health['issues']:
                summary.append(f"  - {issue}")

        summary.append("\n🌐 API STATUS:")
        for api_name, api_status in health['apis'].items():
            status_icon = '✅' if api_status.get('status') else '❌'
            response_time = api_status.get('response_time', 'N/A')
            summary.append(f"  {status_icon} {api_name}: {response_time}s")

        # Media server status
        summary.append("\n🎬 MEDIA SERVERS:")
        for server_name, server_status in health['media_servers'].items():
            if server_status.get('configured', False):
                status_icon = '✅' if server_status.get('status') else '❌'
                if server_name == 'plex' and server_status.get('mappings_valid') is not None:
                    mapping_status = '✅' if server_status.get('mappings_valid') else '❌'
                    summary.append(f"  {status_icon} {server_name.capitalize()} (mappings: {mapping_status})")
                else:
                    summary.append(f"  {status_icon} {server_name.capitalize()}")
            else:
                summary.append(f"  ⚪ {server_name.capitalize()}: Not configured")

        summary.append(f"\n💾 DISK: {health['disk_space'].get('free_gb', 'N/A')}GB free")
        summary.append(f"🧠 MEMORY: {health['system_resources'].get('memory', {}).get('percent', 'N/A')}% used")
        summary.append(f"⚡ CPU: {health['system_resources'].get('cpu', {}).get('percent', 'N/A')}% used")

        # Detailed Plex mapping info if configured
        plex_status = health['media_servers'].get('plex', {})
        if plex_status.get('configured', False) and plex_status.get('mapping_validation'):
            summary.append("\n🔧 PLEX LIBRARY MAPPINGS:")
            for media_type, mapping in plex_status['mapping_validation'].items():
                status_icon = '✅' if mapping['exists'] else '❌'
                summary.append(f"  {status_icon} {media_type}: {mapping['configured_library']}")

        return '\n'.join(summary)