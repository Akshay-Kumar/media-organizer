#!/usr/bin/env python3
"""
porn_filename_parser.py

Improved & defensive porn filename parser with integrated tests.

- Robust to many filename formats.
- Avoids IndexError by checking lengths and types.
- Provides confidence score.
- run_tests() executes a thorough set of examples.
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import datetime
import json

# --- Configurable heuristics ---
PERFORMER_SEPARATORS = [',', '&', 'feat.', 'ft.', ' x ', ' and ', ' + ', ';']
RESOLUTION_PATTERNS = [r'1080p', r'720p', r'2160p', r'4k', r'8k', r'480p', r'540p']
DATE_PATTERNS = [
    r'(?P<Y>\d{4})[-_. ]?(?P<M>\d{2})[-_. ]?(?P<D>\d{2})',  # 2023-07-01 or 20230701 or 2023.07.01
    r'(?P<D>\d{2})[.-](?P<M>\d{2})[.-](?P<Y>\d{4})',  # 01.07.2023 or 01-07-2023 (DD.MM.YYYY)
    r'\b(?P<Y>\d{4})\b'  # Year alone
]
TAG_KEYWORDS = ['uncen', 'uncensored', 'censored', 'remux', 'hdrip', 'webrip', 'cam', 'extended']


# --- Helpers ---
def _clean_separators(s: str) -> str:
    # replace underscores and dots that are used as separators (but keep dots in abbreviations seldom)
    s = s.replace('_', ' ')
    # replace multiple dots used as separators
    s = re.sub(r'\.(?=[^.]*\.)', ' ', s)  # rough attempt to keep single dot if needed
    s = s.replace('.', ' ')
    # collapse multiple spaces
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _split_performers(s: str) -> List[str]:
    s = s.strip()
    if not s:
        return []
    # try to split by separators; choose the separator that yields the most tokens (heuristic)
    best = [s]
    for sep in PERFORMER_SEPARATORS:
        if sep in s:
            parts = [p.strip() for p in s.split(sep) if p.strip()]
            if len(parts) > len(best):
                best = parts
    # filter obvious non-names (years/resolutions)
    filtered = []
    for p in best:
        if re.match(r'^\d{4}$', p):  # year
            continue
        if re.match(r'^(?:' + '|'.join(RESOLUTION_PATTERNS) + r')$', p, flags=re.I):
            continue
        filtered.append(p)
    return filtered


def _extract_resolution(s: str) -> Optional[str]:
    for pat in RESOLUTION_PATTERNS:
        m = re.search(r'\b' + pat + r'\b', s, flags=re.I)
        if m:
            return m.group(0).lower()
    return None


def _extract_date_and_year(s: str) -> Tuple[Optional[str], Optional[int]]:
    for p in DATE_PATTERNS:
        m = re.search(p, s)
        if m:
            gd = m.groupdict()
            try:
                if gd.get('Y') and gd.get('M') and gd.get('D'):
                    y = int(gd['Y']);
                    mo = int(gd['M']);
                    d = int(gd['D'])
                    dt = datetime.date(y, mo, d)
                    return dt.isoformat(), y
                elif gd.get('Y'):
                    y = int(gd['Y'])
                    return None, y
            except Exception:
                continue
    return None, None


def _extract_sku(s: str) -> Optional[str]:
    # conservative SKU extraction: require letter+digit and length between 3..12
    candidates = re.findall(r'\b[A-Z0-9\-]{3,12}\b', s, flags=re.I)
    for c in reversed(candidates):  # prefer end-of-string candidates
        if re.search(r'[A-Za-z]', c) and re.search(r'\d', c):
            # filter out obvious false positives (single year or resolution)
            if re.match(r'^\d{4}$', c):
                continue
            if re.match(r'^(?:' + '|'.join(RESOLUTION_PATTERNS) + r')$', c, flags=re.I):
                continue
            return c
    return None


def _score_result(parsed: Dict[str, Any]) -> float:
    score = 0.0
    if parsed.get('studio'):
        score += 0.15
    if parsed.get('title') and parsed['title'].lower() not in ('', 'unknown', 'unknown title'):
        score += 0.45
    if parsed.get('performers'):
        score += min(0.25, 0.05 * len(parsed['performers']))  # up to 0.25
    if parsed.get('year'):
        score += 0.1
    if parsed.get('resolution'):
        score += 0.05
    return round(min(1.0, score), 2)


# --- Main parser ---
def parse_porn_filename(filename: str) -> Dict[str, Any]:
    original = filename
    # remove directory and extension
    try:
        name = Path(filename).stem
    except Exception:
        name = filename
    name = _clean_separators(name)

    parsed: Dict[str, Any] = {
        'raw': original,
        'studio': '',
        'title': '',
        'performers': [],
        'year': None,
        'date': None,
        'resolution': None,
        'sku': None,
        'tags': [],
        'clean_title': '',
        'confidence': 0.0
    }

    # split by " - " or lone dash with optional spaces
    parts = [p.strip() for p in re.split(r'\s*-\s*', name) if p.strip()]

    # Helper to attach title from leftover tokens
    def leftover_to_title(tokens: List[str]) -> str:
        return ' - '.join(tokens).strip()

    # === Heuristics ===
    if len(parts) >= 4:
        # common: Studio - Title - Performer(s) - Extra
        parsed['studio'] = parts[0]
        parsed['title'] = parts[1]
        # performers often in third; extra may contain resolution/sku/date
        parsed['performers'] = _split_performers(parts[2])
        # look at tail for SKU/day/resolution
        tail = ' '.join(parts[3:])
        if not parsed['sku']:
            parsed['sku'] = _extract_sku(tail)
    elif len(parts) == 3:
        # ambiguous: could be Studio - Title - Performers OR Title - Performers - extra
        # decide by checking whether first part looks like studio (short) or contains 'www'/'site' tokens
        first = parts[0]
        third = parts[2]
        # if third contains performer separators, treat as performers
        if re.search(r'[,&]| and | x ', third, flags=re.I) or len(third.split()) <= 6 and any(
                c.isalpha() for c in third):
            parsed['studio'] = first
            parsed['title'] = parts[1]
            parsed['performers'] = _split_performers(third)
        else:
            # else: studio might be missing; treat as Title - Performer - Extra
            parsed['title'] = parts[0]
            parsed['performers'] = _split_performers(parts[1])
            parsed['sku'] = _extract_sku(parts[2])
    elif len(parts) == 2:
        left, right = parts[0], parts[1]
        # if left looks like a known site token or short studio name (<=3 words), treat as studio-title
        if len(left.split()) <= 3 and re.search(r'[A-Za-z]', left):
            parsed['studio'] = left
            parsed['title'] = right
            # if right seems to contain performers, detect
            if re.search(r'[,&]| and | x ', right, flags=re.I):
                parsed['performers'] = _split_performers(right)
                # try to remove performers from title if misassigned
                parsed['title'] = re.sub(r'\(.+\)$', '', parsed['title']).strip()
        else:
            # fallback: maybe title (left) and performers/extra (right)
            parsed['title'] = left
            parsed['performers'] = _split_performers(right)
            if not parsed['sku']:
                parsed['sku'] = _extract_sku(right)
    else:
        # no dashes — handle parenthetical performers and bracketed info
        working = name
        # extract trailing parenthetical performers e.g. "Title (Jane Doe & John Smith)"
        m_perf = re.search(r'\((?P<perf>[^)]+)\)\s*$', working)
        if m_perf:
            parsed['performers'] = _split_performers(m_perf.group('perf'))
            working = re.sub(r'\([^)]+\)\s*$', '', working).strip()
        # extract bracketed SKU / resolution [1080p]
        parsed['title'] = working

    # tail-token scans (do on full name to catch resolution/year even if in studio/title)
    parsed['resolution'] = _extract_resolution(name)
    dt, yr = _extract_date_and_year(name)
    if dt:
        parsed['date'] = dt
    if yr:
        parsed['year'] = yr
    if not parsed.get('sku'):
        parsed['sku'] = _extract_sku(name)

    # tags
    tags_found = []
    for kw in TAG_KEYWORDS:
        if re.search(r'\b' + re.escape(kw) + r'\b', name, flags=re.I):
            tags_found.append(kw)
    parsed['tags'] = tags_found

    # post-process performers: dedupe, strip, limit to reasonable number
    performers_list = []
    if isinstance(parsed.get('performers'), list):
        for p in parsed['performers']:
            if isinstance(p, str):
                p_clean = p.strip()
                if p_clean and not re.match(r'^\d{4}$', p_clean):
                    performers_list.append(p_clean)
    parsed['performers'] = list(dict.fromkeys(performers_list))  # preserve order unique

    # clean title — remove year/resolution/sku at end
    clean = parsed.get('title', '') or ''
    clean = re.sub(r'\s*\[.*\]\s*$', '', clean).strip()
    clean = re.sub(r'\s*\(.*\)\s*$', '', clean).strip()
    clean = re.sub(r'\b(?:' + '|'.join(RESOLUTION_PATTERNS) + r')\b', '', clean, flags=re.I).strip()
    clean = re.sub(r'\s{2,}', ' ', clean).strip()
    # remove trailing SKU
    if parsed.get('sku'):
        clean = re.sub(re.escape(parsed['sku']) + r'\s*$', '', clean).strip()
    parsed['clean_title'] = clean or parsed.get('title') or ''

    # confidence
    parsed['confidence'] = _score_result(parsed)

    return parsed


# -------------------
# Thorough test harness
# -------------------
TEST_CASES = [
    # Provided examples & edge cases
    "Brazzers - Roxie Sinner - Bullying The House Boy (26.08.2024) rq.mp4",
    "07.09.2024_WEBDL_Yurievij_Please Cum Inside Me 18_Porn Pros_720p.mkv",
    "JUFE-105.mp4",
    "DaniDaniels.com - Dani Daniels Fucks James Deen in The Desert.mkv",
    "My.Stepfather.Stepsister.1080p.2019-07-01-EXT.mp4",
    "JUFE-105 720p.mp4",
    "StudioName - Title Part 1 - Jane Doe & John Smith [1080p] (2022).mp4",
    "PureTaboo - The Lonely Housewife - Jane Doe.mp4",
    "Amazing.Site.Title.with.multiple.dots.and_underscores_720p.mp4",
    "2021-11-15 - Naughty Office - Silvia Saige - Scene 30668.mp4",
    "site.name - Performer1, Performer2 & Performer3 - Title [1080p].mkv",
    "Title only no studio or performer 2020.mp4",
    "Title (Performer A & Performer B).mp4",
    "ABP-123 - Some Japanese Title - 1080p.mkv",
    "Random.Unknown.Format.filewithnosuffix",
    "   Leading and trailing spaces - Site - Name   .mp4",
    "Studio - Title - 2019.mp4",
    "JustTitle.mkv",
    "Some-Site-Title-With-Multiple-Dashes-123-720p.mp4",
    "JaneDoe - Solo - 4k (2021) HD.mp4",
    # add more corner cases
    "(Brazzers) - Title With Parenthesis - John Doe & Jane Smith.mp4",
    "Title [Uncen] (2023).mp4",
    "Studio - Title - SKU12345.mp4",
    "SITE - TITLE - 01.07.2023 - EXTRA.mp4",
]


def run_tests():
    errors = []
    results = []
    for ex in TEST_CASES:
        try:
            parsed = parse_porn_filename(ex)
            # basic structural checks
            assert isinstance(parsed, dict)
            assert 'clean_title' in parsed
            # no field should be None where string expected
            if parsed.get('performers') is None:
                parsed['performers'] = []
            results.append((ex, parsed))
        except Exception as e:
            errors.append((ex, str(e)))

    print("===== PARSER TEST RESULTS =====")
    print(f"Total cases: {len(TEST_CASES)}  Errors: {len(errors)}")
    if errors:
        print("Errors:")
        for ex, err in errors:
            print(f"  - {ex} => {err}")
    else:
        print("No exceptions encountered. Sample outputs:")
        for ex, parsed in results[:10]:
            print("--------------------------------------------------")
            print("File:", ex)
            print(json.dumps(parsed, indent=2, ensure_ascii=False))
    print("===== END TESTS =====")


if __name__ == "__main__":
    run_tests()
