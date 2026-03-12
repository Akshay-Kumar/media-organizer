import fnmatch
import hashlib
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Callable
from typing import List, Optional
from utils.media_extensions import get_media_extensions


class FileUtils:
    @staticmethod
    def get_file_hash(file_path: Path, algorithm: str = 'md5') -> Optional[str]:
        """Calculate file hash for integrity checking with progress tracking"""
        try:
            hash_func = getattr(hashlib, algorithm)()
            file_size = file_path.stat().st_size
            processed = 0

            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_func.update(chunk)
                    processed += len(chunk)

                    # Optional: Log progress for large files
                    if file_size > 10 * 1024 * 1024:  # 10MB+
                        percent = (processed / file_size) * 100
                        if int(percent) % 10 == 0:  # Log every 10%
                            logging.debug(f"Hashing {file_path.name}: {percent:.1f}%")

            return hash_func.hexdigest()
        except Exception as e:
            logging.error(f"Error calculating hash for {file_path}: {e}")
            return None

    @staticmethod
    def get_file_info(file_path: Path) -> Dict[str, Any]:
        """Get comprehensive file information with additional metadata"""
        try:
            stat = file_path.stat()
            file_hash = FileUtils.get_file_hash(file_path)

            return {
                'size': stat.st_size,
                'size_human': FileUtils.format_file_size(stat.st_size),
                'created': datetime.fromtimestamp(stat.st_ctime),
                'modified': datetime.fromtimestamp(stat.st_mtime),
                'accessed': datetime.fromtimestamp(stat.st_atime),
                'hash': file_hash,
                'hash_algorithm': 'md5',
                'permissions': oct(stat.st_mode)[-3:],
                'inode': stat.st_ino,
                'device': stat.st_dev,
                'hard_links': stat.st_nlink,
                'extension': file_path.suffix.lower(),
                'filename': file_path.name,
                'parent_dir': str(file_path.parent),
                'is_symlink': file_path.is_symlink(),
                'is_readable': os.access(file_path, os.R_OK),
                'is_writable': os.access(file_path, os.W_OK),
                'is_executable': os.access(file_path, os.X_OK),
            }
        except Exception as e:
            logging.error(f"Error getting file info for {file_path}: {e}")
            return {}

    @staticmethod
    def safe_operation(operation_func: Callable, *args, max_retries: int = 3, **kwargs) -> bool:
        """Execute a file operation with retry logic"""
        last_exception = None

        for attempt in range(max_retries):
            try:
                return operation_func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    sleep_time = 1 * (2 ** attempt)  # Exponential backoff
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)

        logging.error(f"All {max_retries} attempts failed: {last_exception}")
        return False

    @staticmethod
    def safe_move(src: Path, dst: Path, overwrite: bool = False, max_retries: int = 3) -> bool:
        """Safely move file with error handling and retry logic"""

        def move_operation():
            if not src.exists():
                raise FileNotFoundError(f"Source file does not exist: {src}")

            if dst.exists():
                if overwrite:
                    logging.warning(f"Destination exists, overwriting: {dst}")
                    # Compare files before overwriting
                    if FileUtils.get_file_hash(src) == FileUtils.get_file_hash(dst):
                        logging.info(f"Files are identical, skipping move: {src}")
                        return True
                    dst.unlink()
                else:
                    raise FileExistsError(f"Destination already exists: {dst}")

            # Ensure destination directory exists
            dst.parent.mkdir(parents=True, exist_ok=True)

            # Use shutil.move for atomic operation
            shutil.move(str(src), str(dst))
            logging.info(f"Moved {src} to {dst}")
            return True

        return FileUtils.safe_operation(move_operation, max_retries=max_retries)

    @staticmethod
    def safe_copy(src: Path, dst: Path, overwrite: bool = False, max_retries: int = 3) -> bool:
        """Safely copy file with error handling and retry logic"""

        def copy_operation():
            if not src.exists():
                raise FileNotFoundError(f"Source file does not exist: {src}")

            if dst.exists():
                if overwrite:
                    logging.warning(f"Destination exists, overwriting: {dst}")
                    # Compare files before overwriting
                    if FileUtils.get_file_hash(src) == FileUtils.get_file_hash(dst):
                        logging.info(f"Files are identical, skipping copy: {src}")
                        return True
                    dst.unlink()
                else:
                    raise FileExistsError(f"Destination already exists: {dst}")

            # Ensure destination directory exists
            dst.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(str(src), str(dst))  # copy2 preserves metadata
            logging.info(f"Copied {src} to {dst}")
            return True

        return FileUtils.safe_operation(copy_operation, max_retries=max_retries)

    @staticmethod
    def safe_delete(file_path: Path, max_retries: int = 3) -> bool:
        """Safely delete file with retry logic"""

        def delete_operation():
            if not file_path.exists():
                logging.warning(f"File does not exist, cannot delete: {file_path}")
                return True

            if file_path.is_file():
                file_path.unlink()
                logging.info(f"Deleted file: {file_path}")
            elif file_path.is_dir():
                shutil.rmtree(file_path)
                logging.info(f"Deleted directory: {file_path}")
            return True

        return FileUtils.safe_operation(delete_operation, max_retries=max_retries)

    @staticmethod
    def find_files(
            directory: Path,
            extensions: Optional[List[str]] = None,
            min_size: int = 0,
            max_size: int = 0,
            recursive: bool = True,
            filename_pattern: str = None,
            exclude_patterns: Optional[List[str]] = None
    ) -> List[Path]:
        """Find files with specific criteria including pattern matching"""
        files = []
        exclude_patterns = exclude_patterns or []
        extensions = [ext.lower() for ext in extensions] if extensions else None

        walker = os.walk(directory) if recursive else [(directory, [], os.listdir(directory))]

        for root, _, filenames in walker:
            for name in filenames:
                file_path = Path(root) / name

                # Extension filter
                if extensions and file_path.suffix.lower() not in extensions:
                    continue

                # Size filters
                try:
                    file_size = file_path.stat().st_size
                except (OSError, FileNotFoundError):
                    continue

                if min_size > 0 and file_size < min_size:
                    continue
                if 0 < max_size < file_size:
                    continue

                # Filename pattern (applies only to basename)
                if filename_pattern and not fnmatch.fnmatch(name, filename_pattern):
                    continue

                # Exclude patterns
                if any(fnmatch.fnmatch(name, excl) for excl in exclude_patterns):
                    continue

                files.append(file_path)

        return files

    @staticmethod
    def find_files2(
            directory: Path,
            extensions: Optional[List[str]] = None,
            min_size: int = 0,
            max_size: int = 0,
            recursive: bool = True,
            filename_pattern: str = None,
            exclude_patterns: Optional[List[str]] = None
    ) -> List[Path]:
        """Find files with specific criteria including pattern matching"""
        files = []

        # Glob pattern for recursion
        glob_pattern = "**/*" if recursive else "*"
        exclude_patterns = exclude_patterns or []

        for file_path in directory.glob(glob_pattern):
            if not file_path.is_file():
                continue

            # Extension filter
            if extensions and file_path.suffix.lower() not in [ext.lower() for ext in extensions]:
                continue

            # Size filters
            file_size = file_path.stat().st_size
            if min_size > 0 and file_size < min_size:
                continue
            if 0 < max_size < file_size:
                continue

            # Filename pattern (applies only to basename)
            if filename_pattern and not fnmatch.fnmatch(file_path.name, filename_pattern):
                continue

            # Exclude patterns
            if any(fnmatch.fnmatch(file_path.name, excl) for excl in exclude_patterns):
                continue

            files.append(file_path)

        return files

    @staticmethod
    def cleanup_empty_directories(directory: Path, max_depth: int = 10) -> int:
        """Remove empty directories with depth limit"""
        removed_count = 0
        current_depth = 0

        while current_depth < max_depth:
            found_empty = False

            for root, dirs, files in os.walk(directory, topdown=False):
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    try:
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            removed_count += 1
                            found_empty = True
                            logging.info(f"Removed empty directory: {dir_path}")
                    except OSError as e:
                        logging.debug(f"Could not remove directory {dir_path}: {e}")

            if not found_empty:
                break

            current_depth += 1

        return removed_count

    @staticmethod
    def get_media_extensions() -> List[str]:
        """Return list of common media file extensions"""
        return [
            # Video
            *get_media_extensions().get("video"),
            # Audio
            *get_media_extensions().get("audio"),
            # Images
            # *get_media_extensions().get("image"),
            # Subtitles
            # *get_media_extensions().get("subtitle"),
        ]

    @staticmethod
    def is_valid_media_file(file_path: Path,
                            min_size_mb: int = 1 * 1024,
                            max_size_mb: int = 102400 * 1024,
                            max_sample_size: int = 15 * 1024) -> bool:
        """Check if file is a valid media file with size limits"""
        if not file_path.exists() or not file_path.is_file():
            return False

        # Check extension
        if file_path.suffix.lower() not in FileUtils.get_media_extensions():
            return False

        # Check size limits
        file_size = file_path.stat().st_size
        if file_size < min_size_mb:  # Minimum size
            logging.debug(f"File too small: {file_path} ({file_size} bytes)")
            return False
        if file_size > max_size_mb:  # Maximum size
            logging.debug(f"File too large: {file_path} ({file_size} bytes)")
            return False

        # Skip common junk or sample files
        junk_patterns = ['sample', 'trailer', 'preview', 'extras', 'behindthescenes']
        if any(pat in file_path.stem.lower() for pat in junk_patterns):
            logging.debug(f"Skipped junk/sample file: {file_path}")
            return False

        # skip sample video files, identify samples based on file_size < 10 MB
        if file_path.suffix.lower() in get_media_extensions().get("video") and file_size < max_sample_size:
            return False

        # Skip anime OVA / Special / SP
        '''
        ova_patterns = [r'\bOVA\b', r'\bSpecial\b', r'\bSP\b', r'\bExtra\b']
        if any(re.search(pat, file_path.stem, re.IGNORECASE) for pat in ova_patterns):
            logging.debug(f"Skipped anime special/OVA file: {file_path}")
            return False
        '''

        return True

    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Convert file size to human-readable format"""
        if size_bytes == 0:
            return "0B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)

        while size >= 1024 and i < len(size_names) - 1:
            size /= 1024
            i += 1

        return f"{size:.2f} {size_names[i]}"

    @staticmethod
    def get_directory_size(directory: Path) -> int:
        """Calculate total size of directory contents"""
        total_size = 0
        for file_path in directory.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        return total_size

    @staticmethod
    def compare_files(file1: Path, file2: Path) -> Dict[str, Any]:
        """Compare two files and return differences"""
        if not file1.exists() or not file2.exists():
            return {'error': 'One or both files do not exist'}

        stat1 = file1.stat()
        stat2 = file2.stat()

        hash1 = FileUtils.get_file_hash(file1)
        hash2 = FileUtils.get_file_hash(file2)

        return {
            'size_equal': stat1.st_size == stat2.st_size,
            'size_diff': stat1.st_size - stat2.st_size,
            'modified_equal': stat1.st_mtime == stat2.st_mtime,
            'hash_equal': hash1 == hash2,
            'identical': hash1 == hash2 and stat1.st_size == stat2.st_size,
            'file1_size': stat1.st_size,
            'file2_size': stat2.st_size,
            'file1_hash': hash1,
            'file2_hash': hash2
        }

    @staticmethod
    def create_backup(file_path: Path, backup_dir: Path, suffix: str = None) -> Optional[Path]:
        """Create a backup copy of a file"""
        if not file_path.exists():
            return None

        backup_dir.mkdir(parents=True, exist_ok=True)

        if suffix is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = f"backup_{timestamp}"

        backup_name = f"{file_path.stem}_{suffix}{file_path.suffix}"
        backup_path = backup_dir / backup_name

        if FileUtils.safe_copy(file_path, backup_path, overwrite=True):
            return backup_path
        return None

    @staticmethod
    def get_file_encoding(file_path: Path) -> Optional[str]:
        """Try to detect file encoding"""
        try:
            import chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read(4096)  # Read first 4KB
                result = chardet.detect(raw_data)
                return result['encoding'] if result['confidence'] > 0.7 else None
        except ImportError:
            logging.warning("chardet not installed, cannot detect encoding")
            return None
        except Exception as e:
            logging.debug(f"Error detecting encoding: {e}")
            return None

    @staticmethod
    def validate_path(path: Path, check_readable: bool = True, check_writable: bool = False) -> Dict[str, Any]:
        """Validate path permissions and existence"""
        result = {
            'exists': path.exists(),
            'is_file': path.is_file() if path.exists() else False,
            'is_dir': path.is_dir() if path.exists() else False,
            'is_readable': os.access(path, os.R_OK) if path.exists() else False,
            'is_writable': os.access(path, os.W_OK) if path.exists() else False,
            'is_executable': os.access(path, os.X_OK) if path.exists() else False,
        }

        if path.exists():
            result.update({
                'size': path.stat().st_size,
                'size_human': FileUtils.format_file_size(path.stat().st_size),
                'modified': datetime.fromtimestamp(path.stat().st_mtime),
            })

        return result
