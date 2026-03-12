from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import re
import unicodedata
import os
from core.file_identifier import MediaFileIdentifier


class FileRenamer:
    def __init__(self, config: Dict[str, Any], media_identifier: MediaFileIdentifier):
        self.config = config
        self.media_identifier = media_identifier
        self.logger = logging.getLogger(__name__)

    def generate_new_name(self, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Generate new filename based on media type and metadata, always Windows-safe"""
        media_type = media_info['media_type']

        try:
            # Get the appropriate pattern for this media type
            pattern = self._select_pattern(media_type, media_info, metadata)

            # Format the pattern with actual values
            new_name = self._format_pattern(pattern, media_info, metadata)

            # Clean up and make it Windows-safe
            new_name = self._sanitize_filename(new_name)

            # Validate the generated filename
            if not self.validate_filename(new_name):
                raise ValueError(f"Generated filename is invalid: {new_name}")

            return {
                'new_filename': new_name,
                'pattern_used': pattern,
                'generated_at': datetime.now().isoformat(),
                'media_type': media_type
            }

        except Exception as e:
            self.logger.error(f"Error generating new name: {e}")
            # Fallback to simple naming and sanitize
            fallback_name = self._sanitize_filename(self._create_fallback_name(media_info, metadata))
            return {
                'new_filename': fallback_name,
                'pattern_used': 'fallback',
                'generated_at': datetime.now().isoformat(),
                'media_type': media_type,
                'error': str(e)
            }

    def _select_pattern(self, media_type: str, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        """Select the most appropriate pattern for this media"""
        patterns = self.config['patterns'].get(media_type, [])

        if not patterns:
            self.logger.warning(f"No patterns defined for {media_type}, using fallback")
            return self._get_fallback_pattern(media_type)

        # Choose pattern based on available metadata
        for pattern in patterns:
            if self._pattern_is_appropriate(pattern, media_info, metadata):
                return pattern

        # If no pattern matches, use the first one
        return patterns[0]

    def _pattern_is_appropriate(self, pattern: str, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
        """Check if pattern is appropriate based on available metadata"""
        required_fields = self._get_required_fields_for_pattern(pattern)

        # Check if we have all required fields
        combined_data = {**media_info, **metadata}
        for field in required_fields:
            if field not in combined_data or not combined_data[field]:
                return False

        return True

    def _get_required_fields_for_pattern(self, pattern: str) -> List[str]:
        """Extract required fields from a pattern string"""
        # Simple extraction of {field} patterns
        import re
        fields = re.findall(r'\{(\w+)\}', pattern)
        return list(set(fields))  # Remove duplicates

    def _get_fallback_pattern(self, media_type: str) -> str:
        """Get fallback pattern for media type"""
        fallback_patterns = {
            'movie': "{title} ({year}){extension}",
            'tv_show': "{title} - S{season:02d}E{episode:02d}{extension}",
            'anime': "{title} - S{season:02d}E{episode:02d}{extension}",
            'music': "{track:02d} - {title}{extension}"
        }
        return fallback_patterns.get(media_type, "{title}{extension}")

    def _format_pattern(self, pattern: str, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        """Format filename pattern with actual values"""
        # Combine media_info and metadata, with metadata taking precedence
        values = {**media_info, **metadata}

        # Extract and clean common fields with defaults
        format_values = self._prepare_format_values(values, media_info)

        try:
            return pattern.format(**format_values)
        except KeyError as e:
            self.logger.warning(f"Missing key in pattern: {e}")
            # Try with available values only
            available_values = {k: v for k, v in format_values.items() if k in pattern}
            try:
                return pattern.format(**available_values)
            except KeyError:
                # Fallback to simple pattern if still failing
                return self._create_simple_name(format_values)

    def _prepare_format_values(self, values: Dict[str, Any], media_info: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare all values for pattern formatting, sanitized for Windows filenames"""
        title = self._clean_string(values.get('title', 'Unknown'))
        episode_title = self._clean_string(values.get('episode_title', 'Unknown'))

        # Clean title and capitalize
        title = self.media_identifier.fix_media_title(title=title)

        # media type
        media_type = media_info.get("media_type")

        # Ensure numeric values are integers
        def safe_int(val, default=1):
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        if media_type == "special":
            season = safe_int(values.get('season', 0) or 0)
        else:
            season = safe_int(values.get('season', 1))
        episode = safe_int(values.get('episode', 1))
        track = safe_int(values.get('track', 1))
        disc = safe_int(values.get('disc', 1))

        format_values = {
            'title': title,
            'episode_title': episode_title,
            'year': str(values.get('year', ''))[:4] if values.get('year') else '',
            'season': season,
            'episode': episode,
            'episode_number': f"{episode:02d}",
            'quality': self._clean_string(values.get('quality', '')),
            'resolution': self._clean_string(values.get('resolution', '')),
            'source': self._clean_string(values.get('source', '')),
            'artist': self._clean_string(values.get('artist', 'Unknown Artist')),
            'album': self._clean_string(values.get('album', 'Unknown Album')),
            'track': track,
            'disc': disc,
            'extension': media_info['file_extension'],
            'media_type': media_info['media_type'],
            'release_group': self._clean_string(values.get('release_group', '')),
            'video_codec': self._clean_string(values.get('video_codec', '')),
            'audio_codec': self._clean_string(values.get('audio_codec', '')),
        }

        # Include guessit info if available
        if 'guessit_info' in values:
            guessit_info = values['guessit_info']
            format_values.update({
                'screen_size': self._clean_string(guessit_info.get('screen_size', '')),
                'format': self._clean_string(guessit_info.get('format', '')),
                'audio_channels': self._clean_string(guessit_info.get('audio_channels', '')),
            })

        return format_values

    def _create_simple_name(self, format_values: Dict[str, Any]) -> str:
        """Create a simple filename when pattern formatting fails completely"""
        ext = format_values['extension']

        if format_values.get('title') and format_values.get('episode'):
            return f"{format_values['title']} S{format_values['season']}E{format_values['episode']}{ext}"
        elif format_values.get('title') and format_values.get('year'):
            return f"{format_values['title']} ({format_values['year']}){ext}"
        elif format_values.get('artist') and format_values.get('title'):
            return f"{format_values['artist']} - {format_values['title']}{ext}"
        else:
            return f"renamed{ext}"

    def _create_fallback_name(self, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        """Create a fallback filename when everything else fails"""
        media_type = media_info['media_type']
        ext = media_info['file_extension']

        if media_type == 'movie':
            title = metadata.get('title', media_info.get('title', 'Unknown Movie'))
            year = metadata.get('year', '')
            return f"{self._clean_string(title)} ({year}){ext}" if year else f"{self._clean_string(title)}{ext}"

        elif media_type in ['tv_show', 'anime']:
            title = metadata.get('title', media_info.get('title', 'Unknown Series'))
            season = metadata.get('season', 1)
            episode = metadata.get('episode', 1)
            return f"{self._clean_string(title)} S{season:02d}E{episode:02d}{ext}"

        elif media_type == 'music':
            artist = metadata.get('artist', 'Unknown Artist')
            title = metadata.get('title', 'Unknown Track')
            return f"{self._clean_string(artist)} - {self._clean_string(title, strict_cleanup=True)}{ext}"

        return f"renamed{ext}"

    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters and ensure proper formatting"""
        if not filename:
            return "unknown"

        # Remove invalid characters for Windows/Linux
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '')

        # Replace multiple spaces, dots, and hyphens
        filename = re.sub(r'\s+', ' ', filename)
        filename = re.sub(r'\.+', '.', filename)
        filename = re.sub(r'-+', '-', filename)

        # Remove leading/trailing problematic characters
        filename = filename.strip(' .-_')

        # Ensure the filename is not too long
        if len(filename) > 200:
            filename = filename[:200]

        return filename

    def _clean_string(self, text: Any, strict_cleanup: bool = False) -> str:
        """Clean and normalize text for filenames (Windows-safe)"""
        if text is None:
            return ''

        if not isinstance(text, str):
            text = str(text)

        # Normalize unicode
        text = unicodedata.normalize('NFKD', text)

        # Remove characters invalid in Windows filenames: <>:"/\|?*
        text = re.sub(r'[<>:"/\\|?*]', '', text)

        # Remove other weird characters except basic punctuation
        text = re.sub(r'[^\w\s\-\.\(\)\[\]&,!]', '', text)

        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)

        # Remove leading/trailing spaces and dots
        text = text.strip(' .-')

        # Limit length to 100 characters (for safety)
        if len(text) > 100:
            text = text[:100]

        return text

    def generate_directory_structure(self, media_info: Dict[str, Any], metadata: Dict[str, Any]) -> Path:
        """
        Generate the complete directory structure for the media file,
        returning a Path object safe for Windows and other OSes.
        """
        media_type = media_info['media_type']
        new_name_info = self.generate_new_name(media_info, metadata)
        new_filename = new_name_info['new_filename']

        # Ensure the filename is safe
        new_filename = self._sanitize_filename(new_filename)

        # Split directory parts from any slashes in the pattern (if pattern includes subfolders)
        if '/' in new_filename or '\\' in new_filename:
            # Normalize all slashes to OS separator
            new_filename = new_filename.replace('\\', os.sep).replace('/', os.sep)
            parts = new_filename.split(os.sep)
            filename_only = parts[-1]
            directory_path = Path(*parts[:-1])
        else:
            filename_only = new_filename
            directory_path = Path()

        # Base Plex path for this media type
        base_path = Path(self.config['plex_paths'][media_type])

        # Combine base path + directory structure + filename
        complete_path = base_path / directory_path / filename_only

        # Ensure path is absolute and fully normalized
        return complete_path.resolve()

    def validate_filename(self, filename: str) -> bool:
        """Validate if the generated filename is acceptable"""
        if not filename or filename == 'unknown':
            return False

        # Check for invalid characters
        invalid_chars = '<>:"/\\|?*'
        if any(char in filename for char in invalid_chars):
            return False

        # Check length
        if len(filename) > 255:  # Filesystem limit
            return False

        # Check if it's just extension
        if filename.startswith('.') and len(filename) < 5:
            return False

        return True

    def batch_rename(self, files_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate names for multiple files at once"""
        results = []
        for file_info in files_info:
            try:
                result = self.generate_new_name(file_info['media_info'], file_info['metadata'])
                results.append({
                    'original_path': file_info['media_info']['file_path'],
                    'new_name_info': result,
                    'success': True
                })
            except Exception as e:
                results.append({
                    'original_path': file_info['media_info']['file_path'],
                    'success': False,
                    'error': str(e)
                })
        return results
