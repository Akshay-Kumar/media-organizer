#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import portalocker
from guessit.rules import source
from tqdm import tqdm

from core.file_identifier import MediaFileIdentifier
from core.media_parser import MediaParser
from core.metadata_fetcher import MetadataFetcher
from core.file_renamer import FileRenamer
from core.media_downloader import MediaDownloader
from core.file_mover import FileMover
from core.validator import MediaValidator
from utils.logger import setup_logging, set_log_level, setup_basic_logging
from utils.config_loader import ConfigLoader
from utils.file_utils import FileUtils
from utils.health_check import HealthChecker
from core.library_scanner import LibraryScanner
from utils.torrent_metadata import TorrentMetadata

CELEBRATION_EMOJI = "🎉"
H_LINE_EMOJI = "═"
CHART_EMOJI = "📊"
SUCCESS_EMOJI = "✅"
FAIL_EMOJI = "❌"
TREND_EMOJI = "📈"
CLOCK_EMOJI = "⏱️"
PACKAGE_EMOJI = "📦"
LIGHTNING_EMOJI = "⚡"
GLOBE_EMOJI = "🌐"
WARNING_EMOJI = "⚠️"
MAGNIFIER_EMOJI = "🔍"
CLAPPER_EMOJI = "🎬"
DOCUMENT_EMOJI = "📄"
STOP_EMOJI = "🛑"
EXPLOSION_EMOJI = "💥"


def acquire_file_lock(file_path: Path):
    lock_file = file_path.with_suffix(file_path.suffix + ".lock")
    lock_fp = open(lock_file, 'a+', encoding='utf-8')
    try:
        portalocker.lock(lock_fp, portalocker.LOCK_EX | portalocker.LOCK_NB)
        return lock_fp
    except portalocker.exceptions.LockException:
        lock_fp.close()
        return None

class MediaOrganizer:
    def __init__(self, config: Dict[str, Any]):
        self.config = ConfigLoader.validate_config(config)
        self.logger = logging.getLogger(__name__)
        self.audit_logger = logging.getLogger('audit')
        self.performance_logger = logging.getLogger('performance')

        self.logger.debug("Configuration loaded and validated")
        self.audit_logger.info(f"Plex paths: {self.config['plex_paths']}")

        self.metadata_fetcher = MetadataFetcher(self.config)
        self.media_parser = MediaParser(self.config)
        self.identifier = MediaFileIdentifier(self.config, self.metadata_fetcher, self.media_parser)
        self.renamer = FileRenamer(self.config, self.identifier)
        self.downloader = MediaDownloader(self.config)
        self.mover = FileMover(self.config, self.identifier)
        self.validator = MediaValidator(self.config)
        self.health_checker = HealthChecker(self.config)
        self.library_scanner = LibraryScanner(self.config)
        self.torrent_metadata = TorrentMetadata(self.config)

        self.processed_files = []
        self.start_time = time.time()
        self.logger.info("MediaOrganizer initialized successfully")

        self._run_health_check()

    def _run_health_check(self):
        """Run system health check"""
        health = self.health_checker.run_health_check()
        apis = health.get('apis', {})
        normalized_api_ok = []
        for value in apis.values():
            if isinstance(value, dict):
                normalized_api_ok.append(bool(value.get('status')))
            else:
                normalized_api_ok.append(bool(value))

        if normalized_api_ok and not all(normalized_api_ok):
            self.logger.warning("Some API services are unavailable")
        self.logger.debug(f"Health check: {health}")

    def estimate_remaining_time(self, start_time: float, processed: int, total: int) -> str:
        """Estimate remaining processing time"""
        if processed == 0:
            return "Calculating..."

        elapsed = time.time() - start_time
        time_per_file = elapsed / processed
        remaining_seconds = time_per_file * (total - processed)

        if remaining_seconds > 3600:
            hours = remaining_seconds // 3600
            minutes = (remaining_seconds % 3600) // 60
            return f"{int(hours)}h {int(minutes)}m"
        elif remaining_seconds > 60:
            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            return f"{int(remaining_seconds)}s"

    def process_single_file(self, file_path: Path, pbar: Optional[tqdm] = None, info_hash: str = None, skip_torrent_metadata: bool = False) -> Dict[str, Any]:
        result = self.process_file(file_path, pbar, info_hash=info_hash, media_file_count=1, skip_torrent_metadata=skip_torrent_metadata)
        self.processed_files.append(result)
        file_hash = result.get("file_info", {}).get("hash")
        source = {
            "source": result.get("original_path", str(file_path))
        }
        if result.get('success', False):
            self.torrent_metadata.send_progress_update(
                info_hash,
                file_hash,
                stage="completed",
                progress=100,
                status="completed",
                success=True,
                extra=source
            )
        elif result.get("skipped", False):
            self.torrent_metadata.send_progress_update(
                info_hash,
                file_hash,
                stage="completed",
                progress=100,
                status="skipped",
                success=True,
                extra={
                    **source,
                    "details": result.get(
                        "skip_reason",
                        "Skipped"
                    )
                }
            )
        else:
            self.torrent_metadata.send_progress_update(
                info_hash,
                file_hash,
                stage="completed",
                progress=100,
                status="failed",
                success=False,
                extra=source
            )
        return result

    def process_file(self, file_path: Path, pbar: Optional[tqdm] = None, info_hash: str = None, media_file_count: int = 0, skip_torrent_metadata: bool = False) -> Dict[str, Any]:
        """Process a single media file through all steps with progress updates"""
        file_start_time = time.time()
        file_lock = acquire_file_lock(file_path)
        if not file_lock:
            self.logger.info(f"Skipping {file_path.name} (already being processed by another instance)")
            return {
                'original_path': str(file_path),
                'success': False,
                'errors': ['File locked by another process']
            }
        result = {
            "info_hash": info_hash,
            'original_path': str(file_path),
            'file_info': FileUtils.get_file_info(file_path),
            'success': False,
            'errors': [],
            'warnings': [],
            'processing_time': 0,
            'start_time': datetime.now().isoformat()
        }

        try:
            if pbar:
                pbar.set_description(f"🔄 Validating: {file_path.name[:25]}...")

            min_size = self.config.get('processing', {}).get('min_file_size_mb', 1) * 1024 * 1024
            max_size = self.config.get('processing', {}).get('max_file_size_mb', 102400) * 1024 * 1024
            max_sample_size = self.config.get('processing', {}).get('max_sample_file_size_mb', 15) * 1024 * 1024
            file_hash = result.get("file_info", {}).get("hash")
            source = {
                "source": result.get("original_path", str(file_path))
            }

            if not FileUtils.is_valid_media_file(
                    file_path,
                    min_size,
                    max_size,
                    max_sample_size
            ):
                result["skipped"] = True
                result["skip_reason"] = "Invalid media file"
                result["success"] = False
                result["skip_reason"] = "Invalid media file"
                result["warnings"].append(
                    "Invalid media file"
                )

                return result

            if pbar:
                pbar.set_description(f"🔍 Identifying: {file_path.name[:25]}...")

            self.torrent_metadata.send_progress_update(info_hash, file_hash, "media_info", 0, status="processing", extra=source)
            media_info = self.identifier.identify(file_path, info_hash=info_hash, file_info=result.get("file_info", None), media_file_count=media_file_count, skip_torrent_metadata=skip_torrent_metadata)

            if media_info is not None:
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "media_info", 100, status="completed", success=True, extra=source)
            else:
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "media_info", 100, status="failed", success=False, extra=source)

            if media_info.get("media_type", "Unknown") in ("anime", "tv_show"):
                if media_info.get("season", 1) == 1:
                    if media_info['guessit_info'].get("absolute_number") is not None or media_info['guessit_info'].get("absolute_episode") is not None:
                        media_info['absolute_episode_number'] = media_info['guessit_info'].get("absolute_number") if media_info['guessit_info'].get("absolute_number") is not None else media_info['guessit_info'].get("absolute_episode")
                    else:
                        media_info['absolute_episode_number'] = None
                else:
                    media_info['absolute_episode_number'] = None

            result.update(media_info)

            if not media_info.get('media_type'):
                result['errors'].append('Could not identify media type')
                return result

            if pbar:
                media_type = media_info.get('media_type', 'media')
                pbar.set_description(f"🌐 Fetching {media_type} metadata...")

            # METADATA
            self.torrent_metadata.send_progress_update(info_hash, file_hash, "metadata", 0, status="processing", extra=source)
            metadata = self.metadata_fetcher.fetch_metadata(media_info, info_hash, file_hash)

            if not metadata:
                result['warnings'].append('Could not fetch metadata, using filename-based naming')
                metadata = media_info.copy()
                result['warnings'].append("Metadata fetch failed")
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "metadata", 100, status="failed", success=False, extra=source)
            else:
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "metadata", 100, status="completed", success=True, extra=source)

            metadata['media_type'] = media_info.get('media_type')
            metadata['file_info'] = result.get('file_info')
            result['metadata'] = metadata

            if pbar:
                pbar.set_description(f"📝 Renaming: {file_path.name[:25]}...")

            new_name_info = self.renamer.generate_new_name(media_info, metadata)
            result['new_name_info'] = new_name_info

            if pbar:
                pbar.set_description(f"🚚 Copying to Plex...")

            destination = self.mover.prepare_destination(media_info, metadata, new_name_info)
            source = {
                "source": result.get("original_path", str(file_path)),
                "destination": str(destination)
            }

            copy_result = self.mover.move_file(file_path, destination, metadata, info_hash)
            result['move_result'] = copy_result

            if copy_result.get("success"):
                self.torrent_metadata.send_progress_update(
                    info_hash,
                    file_hash,
                    "copy",
                    100,
                    status="completed",
                    success=True,
                    extra=source
                )
            else:
                result['errors'].append("Copy failed")
                self.torrent_metadata.send_progress_update(
                    info_hash,
                    file_hash,
                    stage="copy",
                    progress=100,
                    status="failed",
                    success=False
                )
                return result

            if self.config.get('download', {}).get('artwork', False) and not self.config.get("dry_run", False):
                if pbar:
                    pbar.set_description(f"🎨 Downloading artwork...")

                # ARTWORK
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "artwork", 0, status="processing", extra=source)
                artwork_paths = self.downloader.download_artwork(metadata, media_info['media_type'], destination, info_hash, file_hash, source)
                result['artwork_paths'] = artwork_paths
                # call your artwork logic
                if artwork_paths:
                    self.torrent_metadata.send_progress_update(info_hash, file_hash, "artwork", 100, status="completed", success=True)
                else:
                    self.torrent_metadata.send_progress_update(info_hash, file_hash, "artwork", 100, status="failed", success=False)

            if self.config.get('download', {}).get('subtitles', False):
                if media_info['media_type'] in ['movie', 'tv_show', 'anime'] and not self.config.get("dry_run", False):
                    if pbar:
                        pbar.set_description(f"📜 Downloading subtitles...")

                    # SUBTITLES
                    self.torrent_metadata.send_progress_update(info_hash, file_hash, "subtitles", 0, status="processing", extra=source)
                    subtitle_paths = self.downloader.download_subtitles(destination, metadata, info_hash, file_hash, source)
                    result['subtitle_paths'] = subtitle_paths
                    if subtitle_paths:
                        # SUBTITLES
                        self.torrent_metadata.send_progress_update(info_hash, file_hash, "subtitles", 100, status="completed", success=True)
                    else:
                        result['warnings'].append('Could not download subtitles')
                        # SUBTITLES
                        self.torrent_metadata.send_progress_update(info_hash, file_hash, "subtitles", 100, status="failed", success=False)

            if pbar:
                pbar.set_description(f"✅ Validating result...")

            # VALIDATION - Start
            self.torrent_metadata.send_progress_update(info_hash, file_hash, "validation", 0, status="processing", extra=source)
            validation = self.validator.validate(result, info_hash, file_hash, source)
            result['validation'] = validation

            if validation['is_valid']:
                result['success'] = True
                # VALIDATION - End
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "validation", 100, status="completed", success=True)

                if self.config.get('library_scan', {}).get('scan_after_each_file', False):
                    # Library scan - Start
                    if pbar:
                        pbar.set_description(f"✅ Library scanning...")
                    # library scan
                    self.torrent_metadata.send_progress_update(info_hash, file_hash, "library_scan", 0, status="processing", extra=source)
                    media_type = metadata.get('media_type')
                    scan_results = self.library_scanner.scan_libraries(media_type, info_hash, file_hash, source)
                    result['library_scan'] = scan_results
                    result['scan_media_type'] = media_type
                    result['scan_duration'] = scan_results.get('duration', 0)

                    if scan_results is not None:
                        # library scan
                        self.torrent_metadata.send_progress_update(info_hash, file_hash, "library_scan", 100, status="completed", success=True, extra=source)
                    else:
                        # library scan
                        self.torrent_metadata.send_progress_update(info_hash, file_hash, "library_scan", 100, status="failed", success=False, extra=source)

                if pbar:
                    pbar.set_description(f"✓ Success: {file_path.name[:25]}...")
                self.audit_logger.info(f"Successfully processed: {file_path.name}")
            else:
                result['errors'].extend(validation.get('errors', []))
                self.torrent_metadata.send_progress_update(info_hash, file_hash, "validation", 100, status="failed", success=False)
                if pbar:
                    pbar.set_description(f"✗ Failed: {file_path.name[:25]}...")

        except Exception as e:
            result['errors'].append(str(e))
            if pbar:
                pbar.set_description(f"💥 Error: {file_path.name[:25]}...")
            self.logger.error(f"Error processing {file_path}: {e}")
            self.logger.exception("Detailed error:")

        finally:
            result['processing_time'] = time.time() - file_start_time
            result['end_time'] = datetime.now().isoformat()
            try:
                self.torrent_metadata.send_processing_report(
                    result
                )
            except Exception:
                self.logger.exception(
                    "Failed to save processing report"
                )
            self.performance_logger.info(f"File processed in {result['processing_time']:.2f}s: {file_path.name}")
            if file_lock:
                try:
                    portalocker.unlock(file_lock)
                finally:
                    file_lock.close()
                # 🔥 CLEANUP LOCK FILE
                lock_path = file_path.with_suffix(file_path.suffix + ".lock")
                if lock_path.exists():
                    try:
                        lock_path.unlink()
                    except Exception as e:
                        self.logger.exception(f"Failed to delete lock file: {lock_path}")

        return result

    def process_directory(self, directory: Path, info_hash: str = None, skip_torrent_metadata:bool = False) -> List[Dict[str, Any]]:
        """Process all media files in a directory with comprehensive progress tracking"""
        processing_config = self.config.get('processing', {})
        min_size = processing_config.get('min_file_size_mb', 1) * 1024 * 1024
        max_size = processing_config.get('max_file_size_mb', 102400) * 1024 * 1024

        media_files = FileUtils.find_files(
            directory,
            extensions=FileUtils.get_media_extensions(),
            min_size=min_size,
            max_size=max_size,
            recursive=True
        )

        self.logger.info(f"Found {len(media_files)} media files to process")

        if not media_files:
            self.logger.warning("No media files found to process")
            return []

        results = []
        start_time = time.time()

        progress_config = self.config.get('progress', {})
        pbar_config = {
            'total': len(media_files),
            'desc': "Initializing...",
            'unit': 'file',
            'unit_scale': False,
            'dynamic_ncols': True,
            'bar_format': '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            'disable': not progress_config.get('enabled', True) or self.config.get('show_progress', True) is False
        }

        with tqdm(**pbar_config) as pbar:
            for i, file_path in enumerate(media_files):
                try:
                    processed = i + 1
                    total = len(media_files)
                    eta = self.estimate_remaining_time(start_time, i, total)

                    success_count = sum(1 for r in results if r.get('success', False))
                    failed_count = len(results) - success_count
                    rate = pbar.format_dict.get('rate')

                    pbar.set_postfix({
                        'success': success_count,
                        'failed': failed_count,
                        'eta': eta,
                        'rate': f"{rate:.1f} files/s" if rate else "N/A"
                    })

                    result = self.process_file(file_path, pbar=pbar, info_hash=info_hash, media_file_count=total, skip_torrent_metadata=skip_torrent_metadata)
                    results.append(result)
                    self.processed_files.append(result)

                    # log progress
                    file_hash = result.get("file_info", {}).get("hash")
                    source = {
                        "source": result.get("original_path", str(file_path))
                    }
                    if result.get('success', False):
                        self.torrent_metadata.send_progress_update(
                            info_hash,
                            file_hash,
                            stage="completed",
                            progress=100,
                            status="completed",
                            success=True,
                            extra=source
                        )
                    elif result.get("skipped", False):
                        self.torrent_metadata.send_progress_update(
                            info_hash,
                            file_hash,
                            stage="completed",
                            progress=100,
                            status="skipped",
                            success=True,
                            extra={
                                **source,
                                "details": result.get(
                                    "skip_reason",
                                    "Skipped"
                                )
                            }
                        )
                    else:
                        self.torrent_metadata.send_progress_update(
                            info_hash,
                            file_hash,
                            stage="completed",
                            progress=100,
                            status="failed",
                            success=False,
                            extra=source
                        )

                    pbar.update(1)

                except KeyboardInterrupt:
                    self.logger.warning("Processing interrupted by user")
                    pbar.set_description("🛑 Interrupted - saving state...")
                    break
                except Exception as e:
                    self.logger.exception(f"Unexpected error processing {file_path}: {e}")
                    failed_result = {
                        'original_path': str(file_path),
                        'success': False,
                        'errors': [f"Unexpected error: {str(e)}"],
                        'file_info': FileUtils.get_file_info(file_path)
                    }
                    results.append(failed_result)
                    pbar.update(1)

        if self.config.get('advanced', {}).get('remove_empty_dirs', True):
            empty_dirs_removed = FileUtils.cleanup_empty_directories(directory)
            if empty_dirs_removed > 0:
                self.logger.info(f"Removed {empty_dirs_removed} empty directories")

        if (
            self.config.get('library_scan', {}).get('enabled', True)
            and not self.config.get('library_scan', {}).get('scan_after_each_file', False)
        ):
            successful_by_type = {}
            for result in results:
                if result.get('success', False):
                    media_type = result.get('media_type', 'unknown')
                    successful_by_type.setdefault(media_type, []).append(result)

            scan_results = {}
            for media_type, files in successful_by_type.items():
                if files:
                    self.logger.info(f"Triggering library scan for {media_type} ({len(files)} files)")
                    scan_result = self.library_scanner.scan_libraries(media_type)
                    scan_results[media_type] = scan_result

            for result in results:
                if result.get('success', False):
                    media_type = result.get('media_type', 'unknown')
                    if media_type in scan_results:
                        result['library_scan'] = scan_results[media_type]
                        result['scan_media_type'] = media_type

        return results

    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive processing report with library scan details"""
        total = len(self.processed_files)
        successful = sum(1 for r in self.processed_files if r.get('success', False))
        failed = total - successful
        total_time = time.time() - self.start_time

        errors_by_type = {}
        processing_times = []
        file_sizes = []

        scan_stats = {
            'total_scans_triggered': 0,
            'successful_scans': 0,
            'failed_scans': 0,
            'scans_by_media_type': {},
            'scans_by_server': {'plex': 0, 'emby': 0},
            'scan_duration': 0
        }

        for result in self.processed_files:
            for error in result.get('errors', []):
                error_type = error.split(':')[0] if ':' in error else error
                errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1

            processing_times.append(result.get('processing_time', 0))
            if 'file_info' in result and 'size' in result['file_info']:
                file_sizes.append(result['file_info']['size'])

            if 'library_scan' in result:
                scan_stats['total_scans_triggered'] += 1
                media_type = result.get('scan_media_type', 'unknown')
                scan_duration = result.get('scan_duration', 0)

                if media_type not in scan_stats['scans_by_media_type']:
                    scan_stats['scans_by_media_type'][media_type] = {
                        'count': 0,
                        'plex_success': 0,
                        'emby_success': 0,
                        'total_duration': 0
                    }
                scan_stats['scans_by_media_type'][media_type]['count'] += 1
                scan_stats['scans_by_media_type'][media_type]['total_duration'] += scan_duration

                scan_data = result['library_scan']

                if scan_data.get('plex'):
                    plex_scan_results = list(scan_data.get('plex'))
                    for scan_result in plex_scan_results:
                        if isinstance(scan_result, dict):
                            for key in scan_result.keys():
                                is_successful_scan = scan_result.get(key)
                                if is_successful_scan:
                                    scan_stats['successful_scans'] += 1
                                    scan_stats['scans_by_server']['plex'] += 1
                                    scan_stats['scans_by_media_type'][media_type]['plex_success'] += 1
                                    scan_stats['scan_duration'] += scan_duration

                if scan_data.get('emby'):
                    emby_scan_results = list(scan_data.get('emby'))
                    for scan_result in emby_scan_results:
                        if isinstance(scan_result, dict):
                            for key in scan_result.keys():
                                is_successful_scan = scan_result.get(key)
                                if is_successful_scan:
                                    scan_stats['successful_scans'] += 1
                                    scan_stats['scans_by_server']['emby'] += 1
                                    scan_stats['scans_by_media_type'][media_type]['emby_success'] += 1
                                    scan_stats['scan_duration'] += scan_duration

                if not scan_data.get('plex') and not scan_data.get('emby'):
                    scan_stats['failed_scans'] += 1

        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        total_size = sum(file_sizes) if file_sizes else 0

        scan_stats['avg_scan_duration'] = (
            scan_stats['scan_duration'] / scan_stats['total_scans_triggered']
            if scan_stats['total_scans_triggered'] > 0 else 0
        )

        api_status = self.metadata_fetcher.get_api_status()
        media_servers_status = {
            'plex': self.health_checker.check_plex_connectivity().get('status', False),
            'emby': self.health_checker.check_emby_connectivity().get('status', False)
        }

        return {
            'total_files': total,
            'successful': successful,
            'failed': failed,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'total_processing_time': total_time,
            'avg_processing_time': avg_processing_time,
            'total_size_bytes': total_size,
            'total_size_human': FileUtils.format_file_size(total_size),
            'errors_by_type': errors_by_type,
            'media_types_processed': self._get_media_type_stats(),
            'api_status': api_status,
            'media_servers_status': media_servers_status,
            'operations_stats': self.mover.get_operations_stats(),
            'library_scan_stats': scan_stats,
            'scan_config': {
                'enabled': self.config.get('library_scan', {}).get('enabled', False),
                'scan_after_each_file': self.config.get('library_scan', {}).get('scan_after_each_file', False),
                'plex_enabled': self.config.get('library_scan', {}).get('plex', {}).get('enabled', False),
                'emby_enabled': self.config.get('library_scan', {}).get('emby', {}).get('enabled', False)
            },
            'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
            'end_time': datetime.now().isoformat(),
            'details': self.processed_files
        }

    def _get_media_type_stats(self) -> Dict[str, Any]:
        """Get detailed statistics about processed media types"""
        stats = {
            'counts': {},
            'success_rates': {},
            'avg_processing_times': {},
            'total_sizes': {}
        }

        media_type_data = {}

        for result in self.processed_files:
            media_type = result.get('media_type', 'unknown')

            if media_type not in media_type_data:
                media_type_data[media_type] = {
                    'count': 0,
                    'success_count': 0,
                    'processing_times': [],
                    'sizes': []
                }

            media_type_data[media_type]['count'] += 1
            media_type_data[media_type]['processing_times'].append(result.get('processing_time', 0))

            if 'file_info' in result and 'size' in result['file_info']:
                media_type_data[media_type]['sizes'].append(result['file_info']['size'])

            if result.get('success', False):
                media_type_data[media_type]['success_count'] += 1

        for media_type, data in media_type_data.items():
            stats['counts'][media_type] = data['count']
            stats['success_rates'][media_type] = (
                data['success_count'] / data['count'] * 100 if data['count'] > 0 else 0
            )
            stats['avg_processing_times'][media_type] = (
                sum(data['processing_times']) / len(data['processing_times'])
                if data['processing_times'] else 0
            )
            stats['total_sizes'][media_type] = sum(data['sizes']) if data['sizes'] else 0

        return stats

    def undo_last_operation(self) -> Dict[str, Any]:
        return self.mover.undo_last_operation()

    def undo_all_operations(self) -> Dict[str, Any]:
        return self.mover.undo_all_operations()


def main():
    parser = argparse.ArgumentParser(description='Media File Organizer - Advanced FileBot Alternative')
    parser.add_argument('source', help='Source file or directory to process')
    parser.add_argument('--config', default='config/config.yaml', help='Config file path')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without copying files')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set logging level')
    parser.add_argument('--log-config', default='config/logging.conf',
                        help='Logging configuration file path')
    parser.add_argument('--no-progress', action='store_true',
                        help='Disable progress bar (useful for scripting)')
    parser.add_argument('--undo', action='store_true',
                        help='Undo the last operation')
    parser.add_argument('--undo-all', action='store_true',
                        help='Undo all operations (use with caution!)')
    parser.add_argument('--health-check', action='store_true',
                        help='Run health check and exit')
    parser.add_argument('--stats', action='store_true',
                        help='Show statistics and exit')
    parser.add_argument('--list-libraries', action='store_true',
                        help='List available Plex/Emby libraries and exit')
    parser.add_argument('--info-hash', help='Info hash of torrent file')
    parser.add_argument('--skip-torrent-metadata', choices=['True', 'False'],
                        default='False',help='Skip fetching metadata from organizerr')

    args = parser.parse_args()

    logging_setup = setup_logging(args.log_config)
    if not logging_setup:
        setup_basic_logging()
        logging.warning("Using fallback basic logging configuration")

    set_log_level(args.log_level)

    config = ConfigLoader.load_config(args.config)

    if args.dry_run:
        config['dry_run'] = True
        logging.info("=== DRY RUN MODE: No files will be copied ===")

    config['show_progress'] = not args.no_progress and sys.stdout.isatty()

    if args.health_check:
        health_checker = HealthChecker(config)
        health = health_checker.run_health_check()

        logging.info("\n" + "=" * 50)
        logging.info("🏥 COMPREHENSIVE HEALTH CHECK")
        logging.info("=" * 50)

        logging.info(f"\n🌐 API STATUS:")
        for api_name, api_status in health['apis'].items():
            if isinstance(api_status, dict):
                ok = api_status.get('status')
                response_time = api_status.get('response_time', 'N/A')
            else:
                ok = bool(api_status)
                response_time = 'N/A'
            status_icon = '✅' if ok else '❌'
            logging.info(f"  {status_icon} {api_name}: {response_time}s")

        logging.info(f"\n🎬 MEDIA SERVERS:")
        for server_name, server_status in health['media_servers'].items():
            if server_status.get('configured', False):
                status_icon = '✅' if server_status.get('status') else '❌'
                if server_name == 'plex' and server_status.get('mappings_valid') is not None:
                    mapping_status = '✅' if server_status.get('mappings_valid') else '❌'
                    logging.info(f"  {status_icon} {server_name.capitalize()} (mappings: {mapping_status})")
                else:
                    logging.info(f"  {status_icon} {server_name.capitalize()}")
            else:
                logging.info(f"  ⚪ {server_name.capitalize()}: Not configured")

        logging.info(f"\n💾 DISK: {health['disk_space'].get('free_gb', 'N/A')}GB free "
              f"({health['disk_space'].get('percent_free', 'N/A')}% free)")
        logging.info(f"🧠 MEMORY: {health['system_resources'].get('memory', {}).get('percent', 'N/A')}% used")
        logging.info(f"⚡ CPU: {health['system_resources'].get('cpu', {}).get('percent', 'N/A')}% used")

        if health.get('issues'):
            logging.info(f"\n⚠️  ISSUES ({len(health['issues'])}):")
            for issue in health['issues']:
                logging.info(f"  - {issue}")

        logging.info(f"\n⏱️  Check completed in {health['check_duration']}s")
        return 0

    source_path = Path(args.source)
    info_hash = args.info_hash
    skip_torrent_metadata = bool(args.skip_torrent_metadata)

    if not source_path.exists():
        logging.error(f"Source path does not exist: {source_path}")
        return 1

    if args.list_libraries:
        library_scanner = LibraryScanner(config)
        libraries = library_scanner.get_available_libraries()

        logging.info("\n" + "=" * 50)
        logging.info("📚 AVAILABLE LIBRARIES")
        logging.info("=" * 50)

        if libraries['plex']:
            logging.info("Plex Libraries:")
            for lib in libraries['plex']:
                logging.info(f"  - {lib}")
        else:
            logging.info("No Plex libraries found or Plex not configured")

        plex_config = config.get('library_scan', {}).get('plex', {})
        if plex_config.get('enabled', False) and plex_config.get('library_mapping'):
            logging.info(f"\n🔧 CONFIGURED PLEX MAPPING:")
            for media_type, library_name in plex_config['library_mapping'].items():
                exists = library_name in libraries['plex'] if libraries['plex'] else False
                status_emoji = '✅' if exists else '❌'
                logging.info(f"  {media_type:10} → {library_name:15} {status_emoji}")

        emby_config = config.get('library_scan', {}).get('emby', {})
        if emby_config.get('enabled', False):
            logging.info(f"\n🎵 Emby: Library scanning enabled")
            if emby_config.get('library_mapping'):
                logging.info(f"Mappings configured for {len(emby_config['library_mapping'])} media types")

        return 0

    # MediaOrganizer Core Object
    organizer = MediaOrganizer(config)

    if args.undo:
        try:
            result = organizer.undo_last_operation()
            logging.info(f"Undo result: {result}")
            return 0 if result.get('success', False) else 1
        finally:
            logging.debug(f"Undo last operation: {result}")

    if args.undo_all:
        try:
            result = organizer.undo_all_operations()
            logging.info(f"Undo all result: {result}")
            return 0 if result.get('success', False) else 1
        finally:
            logging.debug(f"Undo all operation: {result}")

    try:
        if source_path.is_file():
            results = [organizer.process_single_file(source_path, info_hash=info_hash, skip_torrent_metadata=skip_torrent_metadata)]
        else:
            results = organizer.process_directory(source_path, info_hash=info_hash, skip_torrent_metadata=skip_torrent_metadata)

        report = organizer.generate_report()

        logging.info(f"\n{CELEBRATION_EMOJI} PROCESSING COMPLETE")
        logging.info(f"{H_LINE_EMOJI}" * 50)
        logging.info(f"{CHART_EMOJI} Total files: {report['total_files']}")
        logging.info(f"{SUCCESS_EMOJI} Successful: {report['successful']}")
        logging.info(f"{FAIL_EMOJI} Failed: {report['failed']}")
        logging.info(f"{TREND_EMOJI} Success rate: {report['success_rate']:.1f}%")
        logging.info(f"{CLOCK_EMOJI} Total time: {report['total_processing_time']:.2f} seconds")
        logging.info(f"{PACKAGE_EMOJI} Total size: {report['total_size_human']}")
        logging.info(f"{LIGHTNING_EMOJI} Average time per file: {report['avg_processing_time']:.2f}s")

        if report.get('media_types_processed', {}).get('counts'):
            logging.info(f"\n{CLAPPER_EMOJI} MEDIA TYPE STATISTICS")
            logging.info(f"{H_LINE_EMOJI}" * 50)
            for media_type, count in report['media_types_processed']['counts'].items():
                success_rate = report['media_types_processed']['success_rates'].get(media_type, 0)
                avg_time = report['media_types_processed']['avg_processing_times'].get(media_type, 0)
                logging.info(f"  {media_type:10}: {count:3d} files, {success_rate:5.1f}% success, {avg_time:.2f}s avg")

        api_status = report.get('api_status', {})
        if api_status:
            api_status_display = []
            for api_name, status in api_status.items():
                icon = SUCCESS_EMOJI if status else FAIL_EMOJI
                api_status_display.append(f"{api_name}: {icon}")
            logging.info(f"\n{GLOBE_EMOJI} API Status: {', '.join(api_status_display)}")

        media_servers = report.get('media_servers_status', {})
        if media_servers:
            server_status_display = []
            for server_name, status in media_servers.items():
                icon = SUCCESS_EMOJI if status else FAIL_EMOJI
                server_status_display.append(f"{server_name}: {icon}")
            logging.info(f"🏠 Media Servers: {', '.join(server_status_display)}")

        scan_stats = report.get('library_scan_stats', {})
        scan_config = report.get('scan_config', {})

        if scan_config.get('enabled', False) and scan_stats.get('total_scans_triggered', 0) > 0:
            logging.info(f"\n📡 LIBRARY SCAN RESULTS")
            logging.info(f"{H_LINE_EMOJI}" * 50)
            logging.info(f"Total scans triggered: {scan_stats['total_scans_triggered']}")
            logging.info(f"Successful scans: {scan_stats['successful_scans']}")

            if scan_stats['failed_scans'] > 0:
                logging.warning(f"Failed scans: {scan_stats['failed_scans']}")

            logging.info(f"Total scan time: {scan_stats.get('scan_duration', 0):.2f}s")
            logging.info(f"Average scan time: {scan_stats.get('avg_scan_duration', 0):.2f}s")

            if scan_config.get('plex_enabled', False):
                plex_count = scan_stats.get('scans_by_server', {}).get('plex', 0)
                logging.info(f"Plex scans: {plex_count} {SUCCESS_EMOJI if plex_count > 0 else FAIL_EMOJI}")
            if scan_config.get('emby_enabled', False):
                emby_count = scan_stats.get('scans_by_server', {}).get('emby', 0)
                logging.info(f"Emby scans: {emby_count} {SUCCESS_EMOJI if emby_count > 0 else FAIL_EMOJI}")

            if scan_stats.get('scans_by_media_type'):
                logging.info(f"\n🎯 SCANS BY MEDIA TYPE")
                for media_type, stats in scan_stats['scans_by_media_type'].items():
                    plex_success = stats.get('plex_success', 0)
                    emby_success = stats.get('emby_success', 0)
                    total_scans = stats.get('count', 0)
                    avg_duration = stats.get('total_duration', 0) / total_scans if total_scans > 0 else 0

                    plex_status = f"Plex: {plex_success}/{total_scans}" if plex_success > 0 else ""
                    emby_status = f"Emby: {emby_success}/{total_scans}" if emby_success > 0 else ""
                    scan_info = f"{plex_status} {emby_status}".strip()

                    logging.info(f"  {media_type:10}: {total_scans:2d} scans, {avg_duration:.2f}s avg {scan_info}")

        elif scan_config.get('enabled', False):
            logging.info(f"\n📡 Library scanning enabled but no scans triggered")

        if report['failed'] > 0:
            logging.warning(f"\n{WARNING_EMOJI} ERROR SUMMARY")
            logging.warning(f"{H_LINE_EMOJI}" * 50)
            for error_type, count in report['errors_by_type'].items():
                logging.warning(f"{error_type}: {count} occurrences")

            error_files = [r for r in results if r.get('errors')]
            if error_files:
                logging.warning(f"\n{MAGNIFIER_EMOJI} DETAILED ERRORS (first {min(5, len(error_files))} files)")
                logging.warning(f"{H_LINE_EMOJI}" * 50)
                for i, result in enumerate(error_files[:5]):
                    filename = Path(result['original_path']).name
                    logging.warning(f"{i + 1}. {filename}:")
                    for error in result['errors'][:3]:
                        logging.warning(f"   - {error}")
                    if len(result['errors']) > 3:
                        logging.warning(f"   - ... and {len(result['errors']) - 3} more errors")

                if len(error_files) > 5:
                    logging.warning(f"   ... and {len(error_files) - 5} more files with errors")

        ops_stats = report.get('operations_stats', {})
        if ops_stats:
            logging.info(f"\n{CHART_EMOJI} OPERATIONS STATISTICS")
            logging.info(f"{H_LINE_EMOJI}" * 50)
            logging.info(f"Total operations: {ops_stats.get('total_operations', 0)}")
            logging.info(f"Successful: {ops_stats.get('successful_operations', 0)}")
            logging.info(f"Failed: {ops_stats.get('failed_operations', 0)}")
            if ops_stats.get('operations_by_type'):
                logging.info(f"By type: {ops_stats['operations_by_type']}")

        return 0 if report['failed'] == 0 else 1

    except KeyboardInterrupt:
        logging.info(f"\n{STOP_EMOJI} Processing interrupted by user - saving state...")
        return 130

    except Exception as e:
        logging.error(f"{EXPLOSION_EMOJI} Unexpected error during processing: {e}")
        logging.exception("Detailed error traceback:")
        return 1


if __name__ == "__main__":
    exit(main())