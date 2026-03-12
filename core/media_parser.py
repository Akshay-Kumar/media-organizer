import re
from difflib import SequenceMatcher
from typing import Dict, Any, Optional
import logging


# Optional: if you install rapidfuzz, uncomment this
# from rapidfuzz import fuzz

class MediaParser:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def compare_titles(self, name1: str, name2: str) -> float:
        """
        Compare two titles and return similarity score (0–100).
        - Normalizes case and removes punctuation.
        - Uses difflib.SequenceMatcher for ratio.
        - Can be swapped for rapidfuzz if available.
        """
        if not name1 or not name2:
            return 0.0

        def normalize(text: str) -> str:
            return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()

        clean1, clean2 = normalize(name1), normalize(name2)

        # --- Option A: built-in difflib ---
        ratio = SequenceMatcher(None, clean1, clean2).ratio()
        score = ratio * 100

        # --- Option B: if you want better fuzzy scoring ---
        # score = fuzz.ratio(clean1, clean2)  # requires `pip install rapidfuzz`

        return round(score, 2)
