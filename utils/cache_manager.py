import json
import hashlib
from pathlib import Path
from typing import Any, Optional
import logging
from datetime import datetime, timedelta


class CacheManager:
    def __init__(self, cache_dir: str = "cache", ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir)
        self.ttl = timedelta(hours=ttl_hours)
        self.logger = logging.getLogger(__name__)
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_key(self, key_data: Any) -> str:
        """Generate cache key from data"""
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, key_data: Any) -> Optional[Any]:
        """Get cached data"""
        cache_key = self._get_cache_key(key_data)
        cache_file = self.cache_dir / f"{cache_key}.json"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, 'r') as f:
                cache_entry = json.load(f)

            # Check if cache is expired
            cached_time = datetime.fromisoformat(cache_entry['timestamp'])
            if datetime.now() - cached_time > self.ttl:
                cache_file.unlink()
                return None

            return cache_entry['data']

        except Exception as e:
            self.logger.warning(f"Cache read error: {e}")
            return None

    def set(self, key_data: Any, data: Any) -> None:
        """Set cached data"""
        cache_key = self._get_cache_key(key_data)
        cache_file = self.cache_dir / f"{cache_key}.json"

        try:
            cache_entry = {
                'timestamp': datetime.now().isoformat(),
                'data': data,
                'key': key_data
            }

            with open(cache_file, 'w') as f:
                json.dump(cache_entry, f)

        except Exception as e:
            self.logger.warning(f"Cache write error: {e}")

    def clear_expired(self) -> int:
        """Clear expired cache entries"""
        cleared = 0
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r') as f:
                    cache_entry = json.load(f)

                cached_time = datetime.fromisoformat(cache_entry['timestamp'])
                if datetime.now() - cached_time > self.ttl:
                    cache_file.unlink()
                    cleared += 1

            except:
                cache_file.unlink()
                cleared += 1

        return cleared