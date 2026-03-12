import os
from pathlib import Path
from typing import Dict, Any, List
import logging
from utils.file_utils import FileUtils


class MediaValidator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def validate(self, process_result: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive validation of the entire processing result"""
        validation = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'checks_performed': []
        }

        # Check 1: Metadata validation
        self._validate_metadata(process_result, validation)

        # Check 2: File operation validation
        self._validate_file_operations(process_result, validation)

        # Check 3: Destination validation
        self._validate_destination(process_result, validation)

        # Check 4: Integrity validation
        self._validate_integrity(process_result, validation)

        # Check 5: Plex compatibility validation
        self._validate_plex_compatibility(process_result, validation)

        # Determine overall validity
        validation['is_valid'] = len(validation['errors']) == 0

        return validation

    def _validate_metadata(self, result: Dict[str, Any], validation: Dict[str, Any]) -> None:
        """Validate metadata completeness and quality"""
        validation['checks_performed'].append('metadata_validation')

        metadata = result.get('metadata', {})
        media_type = result.get('media_type')

        if not metadata:
            validation['errors'].append('No metadata was fetched')
            return

        # Check for essential fields based on media type
        required_fields = self._get_required_fields(media_type)
        missing_fields = [field for field in required_fields if not metadata.get(field)]

        if missing_fields:
            validation['warnings'].append(f'Missing recommended fields: {", ".join(missing_fields)}')

        # Check data quality
        if media_type == 'movie' and metadata.get('year') and int(metadata['year']) < 1900:
            validation['warnings'].append('Suspicious movie year')

    def _validate_file_operations(self, result: Dict[str, Any], validation: Dict[str, Any]) -> None:
        """Validate file operations were successful"""
        validation['checks_performed'].append('file_operations_validation')

        move_result = result.get('move_result', {})
        if not move_result.get('success', False):
            validation['errors'].append('File move operation failed')

        # Check if source file still exists (should not after successful move)
        source_path = Path(result.get('original_path', ''))
        if source_path.exists() and move_result.get('success', False):
            validation['warnings'].append('Source file still exists after move')

    def _validate_destination(self, result: Dict[str, Any], validation: Dict[str, Any]) -> None:
        """Validate destination file and path"""
        validation['checks_performed'].append('destination_validation')

        move_result = result.get('move_result', {})
        destination = move_result.get('destination')

        if not destination:
            validation['errors'].append('No destination path specified')
            return

        dest_path = Path(destination)

        if not dest_path.exists():
            validation['errors'].append('Destination file does not exist')
            return

        # Check file permissions
        if not os.access(dest_path, os.R_OK):
            validation['errors'].append('Destination file is not readable')

        if not os.access(dest_path, os.W_OK):
            validation['warnings'].append('Destination file is not writable')

        # Check file size (should not be zero)
        if dest_path.stat().st_size == 0:
            validation['errors'].append('Destination file is empty')

    def _validate_integrity(self, result: Dict[str, Any], validation: Dict[str, Any]) -> None:
        """Validate file integrity and consistency"""
        validation['checks_performed'].append('integrity_validation')

        move_result = result.get('move_result', {})
        destination = move_result.get('destination')

        if not destination:
            return

        dest_path = Path(destination)

        # Compare file hashes if available
        original_info = result.get('file_info', {})
        if original_info.get('hash') and dest_path.exists():
            current_hash = FileUtils.get_file_hash(dest_path)
            if current_hash and current_hash != original_info['hash']:
                validation['errors'].append('File integrity check failed - hash mismatch')

    def _validate_plex_compatibility(self, result: Dict[str, Any], validation: Dict[str, Any]) -> None:
        """Validate Plex library compatibility"""
        validation['checks_performed'].append('plex_compatibility_validation')

        media_type = result.get('media_type')
        move_result = result.get('move_result', {})
        destination = move_result.get('destination')

        if not destination:
            return

        dest_path = Path(destination)

        # Check if file is in correct Plex library location
        expected_base = Path(self.config['plex_paths'].get(media_type, ''))
        if expected_base and not str(dest_path).startswith(str(expected_base)):
            validation['warnings'].append('File not in expected Plex library location')

        # Check filename format for Plex compatibility
        if media_type in ['tv_show', 'anime']:
            filename = dest_path.name
            if not any(pattern in filename for pattern in ['S\\d{2}E\\d{2}', '\\d{1,2}x\\d{2}']):
                validation['warnings'].append('TV episode filename may not be Plex-compatible')

    def _get_required_fields(self, media_type: str) -> List[str]:
        """Get required metadata fields for each media type"""
        requirements = {
            'movie': ['title', 'year'],
            'tv_show': ['title', 'season', 'episode'],
            'anime': ['title', 'episode'],
            'music': ['artist', 'title']
        }
        return requirements.get(media_type, [])