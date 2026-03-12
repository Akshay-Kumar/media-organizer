import os
import re
from time import sleep
import requests


class Static:
    """ Static utility methods used by Episode """

    @staticmethod
    def split_file_name(filename):
        """
        takes the file name, returns showname, season, episode and id_style
        based on regex match
        """
        multi_reg = r'[sS][0-9]{1,3}[eE][0-9]{1,3}-?[eE][0-9]{1,3}'
        if re.compile(multi_reg).findall(filename):
            # S01E01-02
            season_id_pattern = re.compile(multi_reg)
            season_id = season_id_pattern.findall(filename)[0]
            get_s_nr = re.compile(r'[0-9]{1,3}')
            season = str(get_s_nr.findall(season_id)[0])
            e_list = get_s_nr.findall(season_id)[1:]
            episode = ' '.join(e_list)
            id_style = 'multi'
        elif re.compile(r'[sS][0-9]{1,3} ?[eE][0-9]{1,3}').findall(filename):
            # S01E01
            season_id_pattern = re.compile(r'[sS]\d{1,3} ?[eE]\d{1,3}')
            season_id = season_id_pattern.findall(filename)[0]
            get_s_nr = re.compile(r'[0-9]{1,3}')
            season = str(get_s_nr.findall(season_id)[0])
            episode = str(get_s_nr.findall(season_id)[1])
            id_style = 'se'
        elif re.compile(r'[0-9]{4}.[0-9]{2}.[0-9]{2}').findall(filename):
            # YYYY.MM.DD
            season_id_pattern = re.compile(r'[0-9]{4}.[0-9]{2}.[0-9]{2}')
            season_id = season_id_pattern.findall(filename)[0]
            season = "NA"
            episode = "NA"
            id_style = 'year'
        elif re.compile(r'0?[0-9][xX][0-9]{1,2}').findall(filename):
            # 01X01
            season_id_pattern = re.compile(r'0?[0-9][xX][0-9]{2}')
            season_id = season_id_pattern.findall(filename)[0]
            get_s_nr = re.compile(r'[0-9]{1,3}')
            season = str(get_s_nr.findall(season_id)[0])
            episode = str(get_s_nr.findall(season_id)[1])
            id_style = 'se'
        elif re.compile(r'[sS][0-9]{1,3}[. ]?[eE][0-9]{1,3}').findall(filename):
            # S01*E01
            season_id_pattern = re.compile(r'[sS]\d{1,3}[. ]?[eE]\d{1,3}')
            season_id = season_id_pattern.findall(filename)[0]
            get_s_nr = re.compile(r'[0-9]{1,3}')
            season = str(get_s_nr.findall(season_id)[0])
            episode = str(get_s_nr.findall(season_id)[1])
            id_style = 'se'
        else:
            # id syle not dealt with
            print('season episode id failed for:')
            print(filename)
            raise ValueError
        return season, episode, season_id, id_style

    @staticmethod
    def showname_encoder(showname):
        """ encodes showname for best possible match """
        # tvmaze doesn't like years in showname
        showname = showname.strip().rstrip('-').rstrip(".").strip().lower()
        year_pattern = re.compile(r'\(?[0-9]{4}\)?')
        year = year_pattern.findall(showname)
        if year and year[0] != showname:
            showname = showname.rstrip(year[0]).strip()
        # find acronym
        acronym = [i for i in showname.split(".") if len(i) == 1]
        # clean up
        encoded = showname.replace(" ", "%20")
        encoded = encoded.replace(".", "%20").replace("'", "%27")
        # put acronym back
        if acronym:
            to_replace = "%20".join(acronym)
            original_acronym = ".".join(acronym)
            encoded = encoded.replace(to_replace, original_acronym)

        return encoded

    @staticmethod
    def tvmaze_request(url):
        """ call the api with back_off on rate limit and user-agent """
        headers = {
            'User-Agent': 'https://github.com/bbilly1/media_organizer'
        }
        response = None
        # retry up to 5 times
        for i in range(5):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.ok:
                    return response.json()
                if response.status_code == 429:
                    print('hit tvmaze rate limiting, slowing down')
                else:
                    print(f'request failed ({response.status_code}) with url:\n{url}')
            except requests.RequestException as e:
                print(f"Request error: {e}")

            # slow down
            back_off = (i + 1) ** 2
            sleep(back_off)

        raise ConnectionError(f"TVMaze request failed after retries: {url}")


class Episode:
    """Represents a single TV episode and fetches metadata from TVMaze."""

    def __init__(self, filename, discovered=None):
        self.filename = filename
        self.file_parsed = self.parse_filename()
        self.discovered = discovered or []
        showname = self.file_parsed['showname']
        show_id = None
        showname_clean = None

        # check discovered shows first
        for entry in self.discovered:
            if showname == entry['showname']:
                show_id = entry['show_id']
                showname_clean = entry['showname_clean']
                break

        self.episode_details = self.get_ep_details(show_id, showname_clean)

    def parse_filename(self):
        """Parse filename to extract show name, season, and episode number."""
        filename = os.path.splitext(self.filename)[0]  # strip extension

        # Remove common release group prefixes and websites
        junk_patterns = [
            r'^(AC|AnimeRG|KaMi|Lucifer22|Sehjada|Toonworld4all)\s+',
            r'www\.[^\s]+',  # websites like www.cpasbien.cm
            r'\b(cpasbien|z-team|mystic)\b',  # extra junk tags
            r'\[.*?\]',  # [Fansub] tags
            r'\(.*?\)',  # (2024), (Eng Sub)
            r'\b(480p|720p|1080p|2160p|4k|web-?dl|bluray|bdrip|hdrip)\b',
            r'\b(x264|x265|hevc|h\.?264|h\.?265)\b',
            r'\b(subbed|dubbed|dual\s*audio)\b'
        ]
        for pat in junk_patterns:
            filename = re.sub(pat, '', filename, flags=re.IGNORECASE)

        # Try S01E01 / 01x01 pattern first
        match = re.search(r'(?:S(\d{1,3})E(\d{1,3})|(\d{1,3})[xX](\d{1,3}))', filename)
        if match:
            season = match.group(1) or match.group(3)
            episode = match.group(2) or match.group(4)
            season_id = match.group(0)
            id_style = 'se'
            showname_raw = filename.split(season_id)[0]

        # Try "- 039" or "episode 34" pattern
        else:
            match = re.search(r'(?:-\s*(\d{1,3})|episode\s*(\d{1,3}))', filename, flags=re.IGNORECASE)
            if match:
                season = '1'  # default season if missing
                episode = match.group(1) or match.group(2)
                season_id = match.group(0)
                id_style = 'number'
                showname_raw = filename.split(season_id)[0]
            else:
                # No number found, treat as standalone show
                season = '1'
                episode = '1'
                season_id = ''
                id_style = 'year'
                showname_raw = filename

        # Cleanup showname
        showname_cleaned = showname_raw
        showname_cleaned = showname_cleaned.split(' - ')[0]  # drop trailing after dash
        showname_cleaned = showname_cleaned.split(' aka ')[0]
        showname_cleaned = showname_cleaned.replace('.', ' ').replace('_', ' ')
        showname_cleaned = re.sub(r'\s+', ' ', showname_cleaned).strip()
        showname_cleaned = re.sub(r'[-_.]+$', '', showname_cleaned).strip()

        # Build parsed dict
        file_parsed = {
            'season': season,
            'episode': episode,
            'season_id': season_id,
            'id_style': id_style,
            'showname': Static.showname_encoder(showname_cleaned),
            'ext': os.path.splitext(self.filename)[1]
        }
        return file_parsed

    def get_show_id(self):
        """Fetch show search results from TVMaze."""
        showname = self.file_parsed['showname']
        url = f'http://api.tvmaze.com/search/shows?q={showname}'
        results = Static.tvmaze_request(url)

        all_results = []
        for idx, res in enumerate(results):
            show = res['show']
            desc_raw = show.get('summary', '')
            desc = re.sub('<[^<]+?>', '', desc_raw) if desc_raw else ''
            all_results.append({
                'list_id': idx,
                'show_id': show['id'],
                'showname_clean': show['name'],
                'status': show.get('status', ''),
                'desc': desc
            })
        return all_results

    def get_ep_details(self, show_id=None, showname_clean=None):
        """Automatically determine show and episode metadata."""
        if not show_id or not showname_clean:
            results = self.get_show_id()
            filename_show = self.file_parsed['showname'].lower()

            match = next((r for r in results if r['showname_clean'].lower() == filename_show), None)
            if match:
                show_id = match['show_id']
                showname_clean = match['showname_clean']
            elif results:
                show_id = results[0]['show_id']
                showname_clean = results[0]['showname_clean']
            else:
                raise ValueError(f"No TVMaze match found for '{self.file_parsed['showname']}'")

        season, episode, episode_name = self.get_episode_name(show_id)
        return {
            'show_id': show_id,
            'showname_clean': showname_clean,
            'season': season,
            'episode': episode,
            'episode_name': episode_name
        }

    def multi_parser(self, show_id):
        """Parse multi-episode filenames."""
        season = self.file_parsed['season']
        episodes = self.file_parsed['episode'].split()
        names = []

        for ep in episodes:
            url = f'http://api.tvmaze.com/shows/{show_id}/episodebynumber?season={season}&number={ep}'
            ep_data = Static.tvmaze_request(url)
            names.append(ep_data['name'])

        return season, '-E'.join(episodes), ', '.join(names)

    def get_episode_name(self, show_id):
        """Fetch episode name based on id_style."""
        id_style = self.file_parsed['id_style']
        ep_data = None

        if id_style == 'multi':
            return self.multi_parser(show_id)

        season = self.file_parsed['season']
        episode = self.file_parsed['episode']

        if id_style in ('se', 'number'):
            url = f'http://api.tvmaze.com/shows/{show_id}/episodebynumber?season={season}&number={episode}'
            ep_data = Static.tvmaze_request(url)

        elif id_style == 'year':
            try:
                year, month, day = self.file_parsed['season_id'].split('.')
                url = f'https://api.tvmaze.com/shows/{show_id}/episodesbydate?date={year}-{month}-{day}'
                episodes_data = Static.tvmaze_request(url)
                ep_data = episodes_data[0] if episodes_data else None
            except Exception as e:
                raise ValueError(f"Invalid year-style episode parsing for {self.filename}: {e}")

        if not ep_data:
            raise ValueError(f"Episode data not found for {self.filename} (style={id_style})")

        ep_name = ep_data['name'].replace('/', '-')
        season_num = str(ep_data['season']).zfill(2)
        episode_num = str(ep_data['number']).zfill(2)

        return season_num, episode_num, ep_name
