import logging
from typing import Dict, Any, Optional, List
import requests
from datetime import datetime, timedelta


class OMDbClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.base_url = "http://www.omdbapi.com/"
        self.api_key = config['api_keys'].get('omdb', '')
        self.session = requests.Session()

        # Cache for search results to reduce API calls
        self._search_cache = {}
        self._cache_expiry = timedelta(hours=1)

    def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make request to OMDB API with error handling"""
        try:
            # Add API key to params
            params_with_key = params.copy()
            params_with_key['apikey'] = self.api_key

            response = self.session.get(self.base_url, params=params_with_key, timeout=30)
            response.raise_for_status()

            data = response.json()

            if data.get('Response') == 'False':
                self.logger.warning(f"OMDB API error: {data.get('Error', 'Unknown error')}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"OMDB API request failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in OMDB request: {e}")
            return None

    def search_movies(self, query: str, year: Optional[int] = None, type_filter: str = "movie") -> Optional[
        List[Dict[str, Any]]]:
        """Search for movies by title"""
        cache_key = f"search_{query}_{year}_{type_filter}"

        # Check cache first
        if cache_key in self._search_cache:
            cached_data, timestamp = self._search_cache[cache_key]
            if datetime.now() - timestamp < self._cache_expiry:
                return cached_data

        params = {
            's': query,
            'type': type_filter
        }

        if year:
            params['y'] = year

        data = self._make_request(params)

        if data and data.get('Search'):
            results = data['Search']
            # Cache the results
            self._search_cache[cache_key] = (results, datetime.now())
            return results

        return None

    def get_movie_by_title(self, title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get movie details by title (exact match)"""
        cache_key = f"title_{title}_{year}"

        # Check cache first
        if cache_key in self._search_cache:
            cached_data, timestamp = self._search_cache[cache_key]
            if datetime.now() - timestamp < self._cache_expiry:
                return cached_data

        params = {
            't': title,
            'plot': 'full',  # Get full plot description
            'type': 'movie'
        }

        if year:
            params['y'] = year

        data = self._make_request(params)

        if data:
            # Cache the result
            self._search_cache[cache_key] = (data, datetime.now())
            return data

        return None

    def get_movie_by_imdb_id(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """Get movie details by IMDb ID (most accurate)"""
        cache_key = f"imdb_{imdb_id}"

        # Check cache first
        if cache_key in self._search_cache:
            cached_data, timestamp = self._search_cache[cache_key]
            if datetime.now() - timestamp < self._cache_expiry:
                return cached_data

        params = {
            'i': imdb_id,
            'plot': 'full'
        }

        data = self._make_request(params)

        if data:
            # Cache the result
            self._search_cache[cache_key] = (data, datetime.now())
            return data

        return None

    def get_movie_details(self, movie_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed movie information - alias for get_movie_by_imdb_id"""
        return self.get_movie_by_imdb_id(movie_id)

    def search_and_get_best_match(self, query: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Search for movies and return the best match with full details"""
        # First try exact title match
        exact_match = self.get_movie_by_title(query, year)
        if exact_match:
            return exact_match

        # If no exact match, search and find best result
        search_results = self.search_movies(query, year)
        if not search_results:
            return None

        # Try to find the best match (simple heuristic - first result)
        if search_results:
            best_match = search_results[0]
            # Get full details for the best match
            if best_match.get('imdbID'):
                return self.get_movie_by_imdb_id(best_match['imdbID'])

        return None

    def format_movie_metadata(self, omdb_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format OMDB data into standardized metadata format"""
        if not omdb_data:
            return {}

        # Parse ratings
        ratings = {}
        if omdb_data.get('Ratings'):
            for rating in omdb_data['Ratings']:
                source = rating.get('Source', '').lower()
                value = rating.get('Value')
                if source and value:
                    ratings[source] = value

        # Parse runtime
        runtime = None
        if omdb_data.get('Runtime') and omdb_data['Runtime'] != 'N/A':
            try:
                runtime = int(omdb_data['Runtime'].replace(' min', ''))
            except ValueError:
                pass

        # Parse year
        year = None
        if omdb_data.get('Year') and omdb_data['Year'] != 'N/A':
            try:
                year_str = omdb_data['Year'].split('–')[0]
                year = int(year_str)
            except (ValueError, IndexError):
                pass

        # Parse box office
        box_office = omdb_data.get('BoxOffice')
        if box_office and box_office != 'N/A':
            try:
                # Remove currency symbols and commas
                box_office = float(box_office.replace('$', '').replace(',', ''))
            except ValueError:
                box_office = None

        return {
            'title': omdb_data.get('Title'),
            'original_title': omdb_data.get('Title'),
            'year': year,
            'rated': omdb_data.get('Rated'),
            'released': omdb_data.get('Released'),
            'runtime': runtime,
            'genres': [genre.strip() for genre in omdb_data.get('Genre', '').split(',')] if omdb_data.get(
                'Genre') else [],
            'director': omdb_data.get('Director'),
            'writers': [writer.strip() for writer in omdb_data.get('Writer', '').split(',')] if omdb_data.get(
                'Writer') else [],
            'actors': [actor.strip() for actor in omdb_data.get('Actors', '').split(',')] if omdb_data.get(
                'Actors') else [],
            'plot': omdb_data.get('Plot'),
            'language': omdb_data.get('Language'),
            'country': omdb_data.get('Country'),
            'awards': omdb_data.get('Awards'),
            'poster': omdb_data.get('Poster') if omdb_data.get('Poster') != 'N/A' else None,
            'ratings': ratings,
            'metascore': int(omdb_data.get('Metascore')) if omdb_data.get('Metascore') and omdb_data[
                'Metascore'] != 'N/A' else None,
            'imdb_rating': float(omdb_data.get('imdbRating')) if omdb_data.get('imdbRating') and omdb_data[
                'imdbRating'] != 'N/A' else None,
            'imdb_votes': self._parse_imdb_votes(omdb_data.get('imdbVotes')),
            'imdb_id': omdb_data.get('imdbID'),
            'type': omdb_data.get('Type'),
            'dvd_release': omdb_data.get('DVD'),
            'box_office': box_office,
            'production': omdb_data.get('Production'),
            'website': omdb_data.get('Website'),
            'source': 'omdb'
        }

    def _parse_imdb_votes(self, votes_str: Optional[str]) -> Optional[int]:
        """Parse IMDb votes string into integer"""
        if not votes_str or votes_str == 'N/A':
            return None

        try:
            # Remove commas from numbers like "1,234,567"
            return int(votes_str.replace(',', ''))
        except ValueError:
            return None

    def search_movie_metadata(self, movie_title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """High-level method to search for movie metadata"""
        movie_data = self.search_and_get_best_match(movie_title, year)
        if movie_data:
            return self.format_movie_metadata(movie_data)
        return None

    def get_movie_metadata_by_imdb(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """High-level method to get movie metadata by IMDb ID"""
        movie_data = self.get_movie_by_imdb_id(imdb_id)
        if movie_data:
            return self.format_movie_metadata(movie_data)
        return None

    def clear_cache(self):
        """Clear the search cache"""
        self._search_cache.clear()

    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about the current cache state"""
        return {
            'cache_size': len(self._search_cache),
            'cache_keys': list(self._search_cache.keys())
        }