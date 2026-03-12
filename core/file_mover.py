import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import portalocker
from core.file_identifier import MediaFileIdentifier
from utils.file_utils import FileUtils


class FileMover:
    def __init__(self, config: Dict[str, Any], identifier: MediaFileIdentifier):
        self.config = config
        self.identifier = identifier
        self.logger = logging.getLogger(__name__)
        self.dry_run = config.get('dry_run', False)

        backup_cfg = config.get('backup', {})
        self.backup_enabled = bool(backup_cfg.get('enabled', True))
        self.backup_dir = Path(str(backup_cfg.get("backup_dir", "backups")))
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.operations_cfg = config.get('operations', {})
        self.operations_file = Path(self.operations_cfg.get('operations_file', 'file_operations.json'))
        self.operations_enabled = bool(self.operations_cfg.get('enabled', True))
        self.operations_file.parent.mkdir(parents=True, exist_ok=True)

        self.operations_lock_file = Path(
            self.operations_cfg.get('operations_lock_file', f"{self.operations_file}.lock")
        )
        self.operations_lock_file.parent.mkdir(parents=True, exist_ok=True)

        self.operations_log: List[Dict[str, Any]] = []
        self._load_operations_log()

    def _with_operations_lock(self):
        """
        Open a dedicated lock file for cross-process synchronization.
        """
        lock_fp = open(self.operations_lock_file, 'a+', encoding='utf-8')
        portalocker.lock(lock_fp, portalocker.LOCK_EX)
        return lock_fp

    def _read_operations_unlocked(self) -> List[Dict[str, Any]]:
        """Read operations file. Caller must already hold lock if concurrency matters."""
        if not self.operations_file.exists():
            return []

        try:
            with open(self.operations_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    return []

                data = json.loads(content)
                if isinstance(data, list):
                    return data

                self.logger.warning(
                    "Operations log file is not a JSON list. Resetting to empty list."
                )
                return []
        except Exception as e:
            self.logger.warning(f"Failed to read operations log: {e}")
            return []

    def _write_operations_unlocked(self, operations: List[Dict[str, Any]]) -> None:
        """Write operations file atomically. Caller must already hold lock."""
        temp_file = self.operations_file.with_suffix(self.operations_file.suffix + ".tmp")

        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(operations, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_file, self.operations_file)

    def _load_operations_log(self):
        """Load operations log from file into memory (best-effort snapshot)."""
        lock_fp = None
        try:
            if not self.operations_enabled:
                self.operations_log = []
                return
            lock_fp = self._with_operations_lock()
            self.operations_log = self._read_operations_unlocked()
            self.logger.info(f"Loaded {len(self.operations_log)} operations from log")
        except Exception as e:
            self.logger.warning(f"Failed to load operations log: {e}")
            self.operations_log = []
        finally:
            if lock_fp:
                try:
                    portalocker.unlock(lock_fp)
                finally:
                    lock_fp.close()

    def _save_operations_log(self):
        """Save in-memory operations log safely."""
        lock_fp = None
        try:
            lock_fp = self._with_operations_lock()
            self._write_operations_unlocked(self.operations_log)
        except Exception as e:
            self.logger.error(f"Failed to save operations log: {e}")
        finally:
            if lock_fp:
                try:
                    portalocker.unlock(lock_fp)
                finally:
                    lock_fp.close()

    def create_backup(self, source_path: Path) -> Optional[Path]:
        """Create backup of original file"""
        if self.dry_run or not self.backup_enabled:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{source_path.stem}_{timestamp}{source_path.suffix}"
        backup_path = self.backup_dir / backup_filename

        try:
            shutil.copy2(source_path, backup_path)
            self.logger.info(f"Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            return None

    def restore_backup(self, backup_path: Path, original_path: Path) -> bool:
        """Restore file from backup"""
        try:
            original_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup_path, original_path)
            self.logger.info(f"Restored from backup: {original_path}")
            return True
        except Exception as e:
            self.logger.error(f"Restore failed: {e}")
            return False

    def prepare_destination(self, media_info: Dict[str, Any], metadata: Dict[str, Any],
                            new_name_info: Dict[str, Any]) -> Path:
        """Prepare destination path for the file with conflict resolution"""
        media_type = media_info['media_type']
        base_path = Path(self.config['plex_paths'][media_type])
        new_filename = new_name_info['new_filename']

        if media_type == 'movie':
            title = metadata.get('title', 'Unknown')
            title = self.identifier.fix_media_title(title=title)
            year = metadata.get('year', '')
            folder_name = f"{self._sanitize_folder_name(title)} ({year})" if year else self._sanitize_folder_name(title)
            destination = base_path / folder_name / new_filename

        elif media_type in ['tv_show', 'anime']:
            series = metadata.get('title', 'Unknown Series')
            series = self.identifier.fix_media_title(title=series)
            season = metadata.get('season', 1)
            season = int(season) if isinstance(season, (int, str)) and str(season).isdigit() else 1
            folder_name = self._sanitize_folder_name(series)
            season_folder = f"Season {season:02d}"
            destination = base_path / folder_name / season_folder / new_filename

        elif media_type == 'special':
            series = metadata.get('title', 'Unknown Series')
            series = self.identifier.fix_media_title(title=series)
            season = metadata.get('season', 0) or 0
            season = int(season) if isinstance(season, (int, str)) and str(season).isdigit() else 0
            folder_name = self._sanitize_folder_name(series)
            season_folder = f"Season {season:02d}"
            destination = base_path / folder_name / season_folder / new_filename

        elif media_type == 'music':
            artist = metadata.get('artist', 'Unknown Artist')
            album = metadata.get('album', 'Unknown Album')
            destination = (
                    base_path
                    / self._sanitize_folder_name(artist)
                    / self._sanitize_folder_name(album)
                    / new_filename
            )

        else:
            destination = base_path / new_filename

        destination = self._resolve_conflicts(destination)
        return destination

    def move_file(self, source_path: Path, destination_path: Path,
                  metadata: Dict[str, Any], info_hash: str) -> Dict[str, Any]:
        """Copy file to destination with comprehensive error handling."""
        result = {
            'source': str(source_path),
            'destination': str(destination_path),
            'success': False,
            'action': 'copy',
            'backup_path': None,
            'timestamp': datetime.now().isoformat()
        }

        try:
            if not source_path.exists():
                result['error'] = 'Source file does not exist'
                self.logger.error(result['error'])
                return result

            backup_path = self.create_backup(source_path)
            result['backup_path'] = str(backup_path) if backup_path else None

            if destination_path.exists():
                result['warning'] = 'Destination file already exists'
                self.logger.warning(result['warning'])

                if self._should_overwrite(source_path, destination_path):
                    result['action'] = 'overwrite'
                else:
                    result['error'] = 'Destination exists and should not be overwritten'
                    self.logger.error(result['error'])
                    return result

            if self.dry_run:
                result['success'] = True
                result['dry_run'] = True
                self.logger.info(f"DRY RUN: Would copy {source_path} to {destination_path}")
                return result

            destination_path.parent.mkdir(parents=True, exist_ok=True)

            if FileUtils.safe_copy(source_path, destination_path, overwrite=True):
                result['success'] = True
                self.logger.info(f"Copied {source_path} to {destination_path}")

                if not destination_path.exists():
                    result['success'] = False
                    result['error'] = 'Copy verification failed - destination file not found'
                    self.logger.error(result['error'])
                else:
                    self._log_operation(result, source_path, destination_path, backup_path, info_hash)
            else:
                result['error'] = 'File copy operation failed'
                self.logger.error(result['error'])

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Error copying file {source_path}: {e}")
            self.logger.exception("Copy error details:")

        return result

    def _log_operation(self, result: Dict[str, Any], source_path: Path,
                       destination_path: Path, backup_path: Optional[Path], info_hash: str):
        """Log a file operation safely across concurrent processes."""
        try:
            if not self.operations_enabled:
                self.logger.debug(f"Skipping log operations.")
                return

            file_hash = None
            if source_path.exists():
                file_hash = FileUtils.get_file_hash(source_path)
            elif destination_path.exists():
                file_hash = FileUtils.get_file_hash(destination_path)

            operation = {
                'operation': result['action'],
                'source': str(source_path),
                'destination': str(destination_path),
                'backup': str(backup_path) if backup_path else None,
                'timestamp': result['timestamp'],
                'success': result['success'],
                'file_size': source_path.stat().st_size if source_path.exists() else (
                    destination_path.stat().st_size if destination_path.exists() else 0
                ),
                'file_hash': file_hash,
                'info_hash': info_hash if isinstance(info_hash, str) else None
            }

            max_operations = int(self.operations_cfg.get('max_operations_log', 1000))

            lock_fp = self._with_operations_lock()
            try:
                operations = self._read_operations_unlocked()
                operations.append(operation)

                if max_operations > 0 and len(operations) > max_operations:
                    operations = operations[-max_operations:]

                self._write_operations_unlocked(operations)
                self.operations_log = operations

                self.logger.info(
                    "Recorded file operation in %s: %s -> %s",
                    self.operations_file,
                    source_path,
                    destination_path
                )
            finally:
                try:
                    portalocker.unlock(lock_fp)
                finally:
                    lock_fp.close()

        except Exception as e:
            self.logger.error(f"Failed to log file operation: {e}")
            self.logger.exception("Operation logging error details:")

    def undo_last_operation(self) -> Dict[str, Any]:
        """Undo the last file operation"""
        result = {
            'success': False,
            'operation': 'undo',
            'message': ''
        }

        self._load_operations_log()

        if not self.operations_log:
            result['message'] = 'No operations to undo'
            return result

        last_op = self.operations_log.pop()

        try:
            if last_op['operation'] in ('copy', 'move') and last_op['success']:
                if Path(last_op['destination']).exists():
                    source_dir = Path(last_op['source']).parent
                    source_dir.mkdir(parents=True, exist_ok=True)

                    if FileUtils.safe_copy(Path(last_op['destination']), Path(last_op['source'])):
                        result['success'] = True
                        result['message'] = f"Undo: Copied back to {last_op['source']}"
                        self.logger.info(result['message'])

                self._save_operations_log()

            elif last_op['operation'] == 'overwrite':
                if last_op.get('backup') and Path(last_op['backup']).exists():
                    if self.restore_backup(Path(last_op['backup']), Path(last_op['destination'])):
                        result['success'] = True
                        result['message'] = f"Undo: Restored from backup to {last_op['destination']}"
                        self.logger.info(result['message'])
                        self._save_operations_log()

        except Exception as e:
            result['message'] = f"Undo operation failed: {e}"
            self.logger.error(result['message'])
            self.operations_log.append(last_op)
            self._save_operations_log()

        return result

    def undo_all_operations(self) -> Dict[str, Any]:
        """Undo all recorded operations (use with caution!)"""
        self._load_operations_log()

        result = {
            'success': True,
            'operations_undone': 0,
            'failed_undos': 0,
            'details': []
        }

        for i in range(len(self.operations_log) - 1, -1, -1):
            operation = self.operations_log[i]
            undo_result = self.undo_last_operation()

            if undo_result['success']:
                result['operations_undone'] += 1
            else:
                result['failed_undos'] += 1
                result['success'] = False

            result['details'].append({
                'operation': operation,
                'undo_result': undo_result
            })

        return result

    def get_operations_stats(self) -> Dict[str, Any]:
        """Get statistics about file operations"""
        self._load_operations_log()
        total = len(self.operations_log)
        successful = sum(1 for op in self.operations_log if op.get('success', False))

        return {
            'total_operations': total,
            'successful_operations': successful,
            'failed_operations': total - successful,
            'last_operation': self.operations_log[-1] if self.operations_log else None,
            'operations_by_type': self._count_operations_by_type()
        }

    def _count_operations_by_type(self) -> Dict[str, int]:
        counts = {}
        for op in self.operations_log:
            op_type = op.get('operation', 'unknown')
            counts[op_type] = counts.get(op_type, 0) + 1
        return counts

    def clear_operations_log(self) -> bool:
        """Clear the operations log safely."""
        lock_fp = None
        try:
            lock_fp = self._with_operations_lock()
            self.operations_log = []
            if self.operations_file.exists():
                self.operations_file.unlink()
            self.logger.info("Operations log cleared")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear operations log: {e}")
            return False
        finally:
            if lock_fp:
                try:
                    portalocker.unlock(lock_fp)
                finally:
                    lock_fp.close()

    def _sanitize_folder_name(self, name: str) -> str:
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '')
        return name.strip()

    def _resolve_conflicts(self, path: Path) -> Path:
        if not path.exists():
            return path

        base_path = path.parent
        stem = path.stem
        suffix = path.suffix

        for i in range(1, 100):
            new_path = base_path / f"{stem} ({i}){suffix}"
            if not new_path.exists():
                return new_path

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return base_path / f"{stem} ({timestamp}){suffix}"

    def _should_overwrite(self, source: Path, destination: Path) -> bool:
        if self.dry_run:
            return False

        if source.stat().st_size != destination.stat().st_size:
            return True

        if source.stat().st_mtime > destination.stat().st_mtime:
            return True

        source_hash = FileUtils.get_file_hash(source)
        dest_hash = FileUtils.get_file_hash(destination)

        if source_hash and dest_hash and source_hash != dest_hash:
            return True

        return False