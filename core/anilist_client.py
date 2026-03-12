import logging
from typing import Dict, Any, Optional
import requests
import re


class AniListClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://graphql.anilist.co"

    def _make_graphql_request(self, query: str, variables: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make GraphQL request to AniList with proper formatting"""
        try:
            payload = {
                'query': query,
                'variables': variables or {}
            }

            response = requests.post(
                self.base_url,
                json=payload,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'User-Agent': 'MediaOrganizer/1.0'
                },
                timeout=30
            )

            # Check for HTTP errors
            response.raise_for_status()

            data = response.json()

            # Check for GraphQL errors
            if data.get('errors'):
                self.logger.warning(f"AniList GraphQL errors: {data['errors']}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"AniList request failed: {e}")
            return None
        except ValueError as e:
            self.logger.warning(f"AniList JSON parse failed: {e}")
            return None

    def search_anime(self, title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Search for anime on AniList with proper GraphQL query"""
        query = """
        query ($search: String, $year: Int) {
            Page (page: 1, perPage: 5) {
                media (search: $search, seasonYear: $year, type: ANIME, sort: SEARCH_MATCH) {
                    id
                    title {
                        romaji
                        english
                        native
                    }
                    description
                    episodes
                    averageScore
                    season
                    seasonYear
                    coverImage {
                        large
                        medium
                    }
                    bannerImage
                    genres
                }
            }
        }
        """

        title = self.remove_year_from_title(title)
        variables = {'search': title}
        if year is not None:
            variables['year'] = year

        result = self._make_graphql_request(query, variables)
        if result and result.get('data') and result['data']['Page']['media']:
            return result['data']['Page']['media'][0]

        return None

    def get_episode_details_by_number(self, anime_id: int, episode_number: int) -> Optional[Dict[str, Any]]:
        """
        Fetch specific episode details by absolute episode number (1-based index),
        automatically paginating through AniList streamingEpisodes pages until found.
        """
        query = """
        query ($id: Int, $page: Int, $perPage: Int) {
            Page(page: $page, perPage: $perPage) {
                pageInfo {
                    total
                    currentPage
                    lastPage
                    hasNextPage
                    perPage
                }
                media(id: $id, type: ANIME) {
                    id
                    title {
                        romaji
                        english
                        native
                    }
                    streamingEpisodes {
                        title
                        thumbnail
                        url
                        site
                    }
                }
            }
        }
        """

        per_page = 50
        page = 1

        while True:
            variables = {'id': anime_id, 'page': page, 'perPage': per_page}
            result = self._make_graphql_request(query, variables)

            if not result or not result.get('data'):
                self.logger.warning(f"No data returned for anime ID {anime_id} on page {page}")
                return None

            page_data = result['data']['Page']
            media_list = page_data.get('media', [])

            if not media_list:
                break

            media = media_list[0]
            episodes = media.get('streamingEpisodes', [])

            if not episodes:
                self.logger.info(f"No streaming episode data found for {media['title']['romaji']}")
                return None

            # Find episode in episodes by episode number
            ep = self.find_episode_by_number(episodes, episode_number)
            if ep:
                return {
                    'title': media['title']['english'] or media['title']['romaji'] or media['title']['native'],
                    'episode': episode_number,
                    'episode_title': ep.get('title') or f"Episode {episode_number}",
                    'thumbnail': ep.get('thumbnail'),
                    'stream_url': ep.get('url'),
                    'source_site': ep.get('site')
                }
            if not page_data['pageInfo']['hasNextPage']:
                break

            page += 1

        self.logger.info(f"Episode number {episode_number} not found for anime ID {anime_id}")
        return None

    def get_anime_details(self, anime_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed anime information, including streaming episode titles if available"""
        query = """
        query ($id: Int) {
            Media (id: $id, type: ANIME) {
                id
                title {
                    romaji
                    english
                    native
                }
                description
                episodes
                duration
                averageScore
                season
                seasonYear
                coverImage {
                    large
                    medium
                }
                bannerImage
                genres
                tags {
                    name
                }
                relations {
                    edges {
                        relationType
                        node {
                            id
                            title {
                                romaji
                                english
                            }
                        }
                    }
                }
                # ✅ Episode-level info (if available)
                streamingEpisodes {
                    title
                    thumbnail
                    url
                    site
                }
            }
        }
        """

        variables = {'id': anime_id}
        result = self._make_graphql_request(query, variables)
        if result and result.get('data') and result['data']['Media']:
            return result['data']['Media']

        return None

    def format_anime_metadata(self, anime_data: Dict[str, Any], episode: int = None) -> Dict[str, Any]:
        """Format AniList data into standard metadata format"""
        title = (
                anime_data['title']['english'] or
                anime_data['title']['romaji'] or
                anime_data['title']['native']
        )

        # Clean description (remove HTML tags and truncate)
        description = ''
        if anime_data.get('description'):
            import re
            description = re.sub('<br>', '\n', anime_data['description'])
            description = re.sub('<[^<]+?>', '', description)  # Remove all HTML tags
            description = description[:500]  # Truncate

        return {
            'title': title,
            'episode_title': f"Episode {episode}" if episode else "Unknown Episode",
            'episode': episode,
            'overview': description,
            'rating': anime_data.get('averageScore'),
            'genres': anime_data.get('genres', []),
            'year': anime_data.get('seasonYear'),
            'season': anime_data.get('season'),
            'total_episodes': anime_data.get('episodes'),
            'poster_path': anime_data['coverImage']['large'] if anime_data.get('coverImage') else None,
            'backdrop_path': anime_data.get('bannerImage'),
            'source': 'anilist',
            'anilist_id': anime_data['id']
        }

    def format_episode_metadata(self, anime_data: Dict[str, Any], episode_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format AniList episode data into standard metadata format"""
        anime_title = episode_data.get("title")
        episode_title = episode_data.get("episode_title")
        episode_number = int(episode_data.get("episode"))

        # Clean description (remove HTML tags and truncate)
        description = ''
        if anime_data.get('description'):
            import re
            description = re.sub('<br>', '\n', anime_data['description'])
            description = re.sub('<[^<]+?>', '', description)  # Remove all HTML tags
            description = description[:500]  # Truncate

        return {
            'title': anime_title,
            'episode_title': episode_title,
            'episode': episode_number,
            'overview': description,
            'rating': anime_data.get('averageScore'),
            'genres': anime_data.get('genres', []),
            'year': anime_data.get('seasonYear'),
            'season': 0 if isinstance(anime_data.get('season'), str) else int(anime_data.get('season')),
            'total_episodes': anime_data.get('episodes'),
            'poster_path': anime_data['coverImage']['large'] if anime_data.get('coverImage') else None,
            'backdrop_path': anime_data.get('bannerImage'),
            'source': 'anilist',
            'anilist_id': anime_data['id']
        }

    # Simple standalone function for basic AniList queries
    def query_anilist_simple(self, title: str) -> Optional[Dict[str, Any]]:
        """Simple AniList query function for basic testing"""
        try:
            query = """
            query ($search: String) {
                Page (page: 1, perPage: 1) {
                    media (search: $search, type: ANIME) {
                        id
                        title {
                            romaji
                            english
                        }
                    }
                }
            }
            """

            response = requests.post(
                "https://graphql.anilist.co",
                json={'query': query, 'variables': {'search': title}},
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if data.get('data') and data['data']['Page']['media']:
                return data['data']['Page']['media'][0]

        except Exception as e:
            logging.warning(f"Simple AniList query failed: {e}")

        return None

    def clean_episode_title(self, title: str) -> str:
        """
        Normalize episode title by removing prefixes like 'Episode 12', 'Ep. 03 -', etc.
        Example:
            'Episode 31 - The Legacy' → 'The Legacy'
            'Ep.12: The Storm' → 'The Storm'
            'Episode 3' → ''
        """
        if not title:
            return ""

        title = title.strip()

        # Remove common episode prefixes
        cleaned = re.sub(
            r'^\s*(episode|ep)[\.\s_-]*\d{1,3}[\s:\-–]*',
            '',
            title,
            flags=re.IGNORECASE
        )

        # Remove any remaining numeric-only start (e.g., "12 - The Show Begins")
        cleaned = re.sub(r'^\s*\d{1,3}[\s:\-–]*', '', cleaned)

        return cleaned.strip()

    def remove_year_from_title(self, title: str) -> str:
        # Removes patterns like "(2023)" or "(1999)"
        return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()

    def find_episode_by_number(self, episodes, episode_number):
        """
        Finds an episode dictionary in the list by episode number.
        Matches patterns like 'Episode 130 - ...'
        """
        for ep in episodes:
            match = re.search(r'Episode\s+(\d+)', ep['title'])
            if match:
                num = int(match.group(1))
                if num == episode_number:
                    return ep
        return None
