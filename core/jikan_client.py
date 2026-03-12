import requests
import logging
from typing import Dict, Any, Optional
import re

class AnimeEpisodeFetcher:
    BASE_URL = "https://api.jikan.moe/v4"

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    def search_anime(self, title: str) -> Optional[Dict[str, Any]]:
        """Search anime by title using Jikan v4"""
        try:
            response = requests.get(
                f"{self.BASE_URL}/anime",
                params={'q': title, 'limit': 5},
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            if data.get('data'):
                return data['data'][0]  # Return first match
        except Exception as e:
            self.logger.warning(f"Anime search failed for '{title}': {e}")
        return None

    def search_anime2(self, title: str) -> Optional[Dict[str, Any]]:
        try:
            # remove year from anime title if present
            title = self.remove_year_from_title(title=title)
            response = requests.get(f"{self.BASE_URL}/anime", params={"q": title, "limit": 10}, timeout=15)
            response.raise_for_status()
            data = response.json().get("data", [])

            # Prefer entries whose title_english or title contains the exact title query
            for anime in data:
                all_titles = [
                    anime.get("title", "").lower(),
                    anime.get("title_english", "").lower(),
                    # anime.get("title_japanese", "").lower(),
                ]
                # if title.lower() in all_titles or title.lower() in anime.get("title_english", "").lower():
                if title.lower() in all_titles:
                    return anime

            # Fallback: return first result
            return data[0] if data else None

        except Exception as e:
            self.logger.warning(f"Anime search failed for '{title}': {e}")
            return None

    def get_episode_details(self, anime_title: str, episode_number: int,
                            anime_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch precise anime episode details by title + episode number using Jikan v4.

        Args:
            anime_title (str): Anime title
            episode_number (int): episode number
            anime_data (Dict): Anime data

        Returns:
            dict or None: Episode metadata
        """
        if not anime_data:
            self.logger.info(f"No anime found for '{anime_title}'.")
            return None

        mal_id = anime_data['mal_id']

        # Fetch episode details
        try:
            response = requests.get(f"{self.BASE_URL}/anime/{mal_id}/episodes", params={'page': 1}, timeout=15)
            response.raise_for_status()
            episodes_data = response.json().get('data', [])

            # Pagination: Jikan v4 returns paginated episode data
            while episodes_data:
                for ep in episodes_data:
                    if ep.get('mal_id') and (ep.get('mal_id') == episode_number or ep.get('number') == episode_number):
                        return {
                            'anime_id': mal_id,
                            'anime_title': anime_data.get('title'),
                            'episode_number': ep.get('number'),
                            'episode_title': ep.get('title'),
                            'aired': ep.get('aired'),
                            'synopsis': ep.get('synopsis'),
                            'image_url': ep.get('images', {}).get('jpg', {}).get('image_url'),
                            'total_episodes': anime_data.get('episodes'),
                            'genres': [g['name'] for g in anime_data.get('genres', [])],
                            'source': 'mal'
                        }

                # Check if there is a next page
                pagination = response.json().get('pagination', {})
                if pagination.get('has_next_page'):
                    next_page = pagination.get('current_page', 1) + 1
                    response = requests.get(f"{self.BASE_URL}/anime/{mal_id}/episodes", params={'page': next_page},
                                            timeout=15)
                    response.raise_for_status()
                    episodes_data = response.json().get('data', [])
                else:
                    break

        except Exception as e:
            self.logger.warning(f"Fetching episodes failed for '{anime_title}': {e}")

        self.logger.info(f"Episode {episode_number} not found for '{anime_title}'.")
        return None

    def get_episode_details2(
            self,
            anime_title: str,
            episode_number: int,
            anime_data: Dict[str, Any] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch precise anime episode details by title + episode number using Jikan v4.

        Args:
            anime_title (str): Anime title
            episode_number (int): Episode number within the season
            anime_data (Dict): Anime metadata (from search_anime)

        Returns:
            dict or None: Episode metadata
        """
        if not anime_data:
            self.logger.info(f"No anime found for '{anime_title}'.")
            return None

        mal_id = anime_data["mal_id"]

        try:
            page = 1
            while True:
                response = requests.get(
                    f"{self.BASE_URL}/anime/{mal_id}/episodes",
                    params={"page": page},
                    timeout=15,
                )
                response.raise_for_status()
                payload = response.json()
                episodes_data = payload.get("data", [])

                for ep in episodes_data:
                    # ✅ Compare using mal_id and episode number
                    if ep.get("number") or ep.get("mal_id"):
                        if ep.get("number") == episode_number or ep.get("mal_id") == episode_number:
                            return {
                                "anime_id": mal_id,
                                "title": anime_data.get("title_english"),
                                "season": 0,
                                "episode": ep.get("number") or ep.get("mal_id") or 0,
                                "episode_title": ep.get("title"),
                                "aired": ep.get("aired"),
                                "synopsis": ep.get("synopsis"),
                                "image_url": ep.get("images", {}).get("jpg", {}).get("image_url"),
                                "total_episodes": anime_data.get("episodes"),
                                "genres": [g["name"] for g in anime_data.get("genres", [])],
                                "source": "jikan",
                            }

                # Pagination handling
                pagination = payload.get("pagination", {})
                if pagination.get("has_next_page"):
                    page += 1
                else:
                    break

        except Exception as e:
            self.logger.warning(f"Fetching episodes failed for '{anime_title}': {e}")

        self.logger.info(f"Episode {episode_number} not found for '{anime_title}'.")
        return None

    def remove_year_from_title(self, title: str) -> str:
        # Removes patterns like "(2023)" or "(1999)"
        return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()

