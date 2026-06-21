import re
import unicodedata
from rapidfuzz import fuzz
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime


class TitleMatcher:
    def __init__(self, weights: Optional[Dict[str, int]] = None, tie_margin: float = 2.0, debug: bool = False):
        # Default weights (used as base)
        self.default_weights = {
            "year_match": 10,
            "year_proximate": 5,
            "year_mismatch": -15,
            "episode_match": 20,
            "season_match_episode_mismatch": -10,
            "season_mismatch": -20,
            "title_match_boost": 5,
        }

        # Movie-specific weight profile
        self.movie_weights = {
            "year_match": 15,
            "year_proximate": -15,
            "year_mismatch": -30,
            "episode_match": 0,
            "season_match_episode_mismatch": 0,
            "season_mismatch": -10,
            "title_match_boost": 10,
        }

        # Series-specific weight profile
        self.series_weights = {
            "year_match": 8,
            "year_proximate": 5,
            "year_mismatch": -10,
            "episode_match": 25,
            "season_match_episode_mismatch": -10,
            "season_mismatch": -20,
            "title_match_boost": 5,
        }

        self.user_weights = weights or {}
        self.tie_margin = tie_margin
        self.debug = debug
        self.common_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'
        }

    # ----------------------------
    # Normalization
    # ----------------------------
    def normalize_title(self, title: str) -> str:
        title = title.lower()
        title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("utf-8")
        title = re.sub(r"[^a-z0-9\s]", " ", title)
        title = re.sub(r"\s+", " ", title).strip()
        words = [word for word in title.split() if word not in self.common_words]
        return " ".join(words)

    def extract_year(self, title: str) -> Optional[int]:
        current_year = datetime.now().year
        matches = re.findall(r"\b(19\d{2}|20[0-2]\d)\b", title)
        for match in matches:
            year = int(match)
            if 1900 <= year <= current_year + 2:
                return year
        return None

    def extract_season_episode(self, filename: str) -> Tuple[Optional[int], Optional[int]]:
        patterns = [
            r"[Ss](\d{1,2})[Ee](\d{1,2})",
            r"(\d{1,2})x(\d{1,2})",
            r"season[\s\.]?(\d{1,2})[\s\.]?episode[\s\.]?(\d{1,2})",
            r"(\d)(\d{2})",
            r"[Ss](\d{1,2})[\s\.\-]?[Ee](\d{1,2})",
        ]
        normalized_filename = filename.lower()
        for pattern in patterns:
            match = re.search(pattern, normalized_filename)
            if match:
                try:
                    season, episode = int(match.group(1)), int(match.group(2))
                    if 1 <= season <= 50 and 1 <= episode <= 100:
                        return season, episode
                except (ValueError, IndexError):
                    continue
        return None, None

    # ----------------------------
    # Helpers
    # ----------------------------
    def _get_weights(self, content_type: str) -> Dict[str, int]:
        """Return merged weights based on content_type ('movie' or 'series')."""
        base = self.movie_weights if content_type == "movie" else self.series_weights
        # Allow user override
        return {**base, **self.user_weights}

    def adjust_for_year(self, score: float, file_year: Optional[int], candidate_year: Optional[int],
                        weights: Dict[str, int]) -> Tuple[float, float]:
        adjustment = 0
        if file_year and candidate_year:
            year_diff = abs(file_year - candidate_year)
            if year_diff == 0:
                adjustment = weights["year_match"]
            elif year_diff <= 2:
                adjustment = weights["year_proximate"]
            else:
                adjustment = weights["year_mismatch"]
        score += adjustment
        return score, adjustment

    def adjust_for_episode(self, score: float, file_season: Optional[int], file_episode: Optional[int],
                           candidate_season: Optional[int], candidate_episode: Optional[int],
                           weights: Dict[str, int]) -> Tuple[float, float]:
        adjustment = 0
        if file_season and file_episode:
            if (file_season == candidate_season) and (file_episode == candidate_episode):
                adjustment = weights["episode_match"]
            elif file_season == candidate_season:
                adjustment = weights["season_match_episode_mismatch"]
            else:
                adjustment = weights["season_mismatch"]
        score += adjustment
        return score, adjustment

    def title_similarity2(self, query: str, candidate: str, weights: Dict[str, int],
                          content_type: str = "movie") -> float:
        def remove_years(text: str) -> str:
            return re.sub(r"[\(\[\{\-]?(19\d{2}|20[0-2]\d)[\)\]\}\-]?", "", text)

        if content_type == "movie":
            query = remove_years(query)
            candidate = remove_years(candidate)

        query_norm = self.normalize_title(query)
        candidate_norm = self.normalize_title(candidate)

        if not query_norm or not candidate_norm:
            return 0.0

        query_tokens = query_norm.split()
        candidate_tokens = candidate_norm.split()
        query_set = set(query_tokens)
        candidate_set = set(candidate_tokens)

        ratio_score = fuzz.ratio(query_norm, candidate_norm)
        token_sort = fuzz.token_sort_ratio(query_norm, candidate_norm)
        partial = fuzz.partial_ratio(query_norm, candidate_norm)

        combined_score = (ratio_score * 0.55) + (token_sort * 0.30) + (partial * 0.15)

        if query_norm == candidate_norm:
            combined_score += weights.get("title_match_boost", 0)

        if query_set < candidate_set or candidate_set < query_set:
            combined_score -= 20

        if len(query_tokens) == 1 and len(candidate_tokens) > 1 and query_norm != candidate_norm:
            combined_score -= 15

        if abs(len(query_tokens) - len(candidate_tokens)) >= 1:
            combined_score -= abs(len(query_tokens) - len(candidate_tokens)) * 4

        return max(0, min(100, combined_score))

    def title_similarity(self, query: str, candidate: str, weights: Dict[str, int]) -> float:
        query_norm = self.normalize_title(query)
        candidate_norm = self.normalize_title(candidate)
        token_set = fuzz.token_set_ratio(query_norm, candidate_norm)
        token_sort = fuzz.token_sort_ratio(query_norm, candidate_norm)
        partial = fuzz.partial_ratio(query_norm, candidate_norm)
        combined_score = (token_set * 0.5) + (token_sort * 0.3) + (partial * 0.2)
        if query_norm == candidate_norm:
            combined_score += weights["title_match_boost"]
        return min(100, combined_score)

    # ----------------------------
    # Main scoring
    # ----------------------------
    def compute_match_score(
        self,
        file_title: str,
        candidate: Dict[str, Any],
        file_year: Optional[int],
        file_season: Optional[int],
        file_episode: Optional[int],
        content_type: str = "movie"
    ) -> Tuple[float, Dict[str, float]]:
        weights = self._get_weights(content_type)

        base_score = self.title_similarity2(file_title, candidate["title"], weights, content_type=content_type)
        debug_info = {"base_title_score": base_score}

        if base_score < 40:
            base_score *= 0.5
            debug_info["base_title_score"] = base_score

        score, year_adj = self.adjust_for_year(base_score, file_year, candidate.get("year"), weights)
        debug_info["year_adjustment"] = year_adj
        episode_adj = 0

        if content_type == "series":
            score, episode_adj = self.adjust_for_episode(
                score, file_season, file_episode,
                candidate.get("season"), candidate.get("episode"),
                weights
            )
        elif content_type == "movie":
            # Penalize if candidate has season/episode (false positive)
            if candidate.get("season") or candidate.get("episode"):
                score += weights["season_mismatch"] / 2

        debug_info["episode_adjustment"] = episode_adj
        debug_info["final_score"] = max(0, min(100, score))
        return debug_info["final_score"], debug_info

    # ----------------------------
    # Tie-breaking
    # ----------------------------
    def _tie_break_candidates(self, file_title: str, file_year: Optional[int],
                              file_season: Optional[int], file_episode: Optional[int],
                              candidates: List[Dict[str, Any]], content_type: str) -> Dict[str, Any]:
        def priority(cand: Dict[str, Any]):
            episode_score = 0
            if content_type == "series" and file_season and file_episode:
                if cand.get("season") == file_season and cand.get("episode") == file_episode:
                    episode_score = 2
                elif cand.get("season") == file_season:
                    episode_score = 1
            year_score = 0
            if file_year and cand.get("year"):
                if file_year == cand["year"]:
                    year_score = 2
                elif abs(file_year - cand["year"]) == 1:
                    year_score = 1
            title_score = fuzz.token_set_ratio(self.normalize_title(file_title),
                                               self.normalize_title(cand["title"]))
            return episode_score, year_score, title_score

        candidates.sort(key=priority, reverse=True)
        return candidates[0]

    # ----------------------------
    # Public API
    # ----------------------------
    def match(self, file_title: str, api_results: List[Dict[str, Any]],
              threshold: int = 75, content_type: str = "movie"
              ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        if not api_results:
            return None, []

        file_year = self.extract_year(file_title)
        file_season, file_episode = (None, None)
        if content_type == "series":
            file_season, file_episode = self.extract_season_episode(file_title)

        scored_results = []
        for r in api_results:
            score, debug_info = self.compute_match_score(
                file_title, r, file_year, file_season, file_episode, content_type
            )
            r_copy = {**r, "score": score}
            if self.debug:
                r_copy["debug"] = debug_info
            scored_results.append(r_copy)

        scored_results.sort(key=lambda x: x["score"], reverse=True)

        if not scored_results or scored_results[0]["score"] < threshold:
            return None, scored_results

        top_score = scored_results[0]["score"]
        top_candidates = [c for c in scored_results if (top_score - c["score"]) <= self.tie_margin]
        best = self._tie_break_candidates(file_title, file_year, file_season, file_episode, top_candidates, content_type)
        return best, scored_results
