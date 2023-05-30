import random
import pandas as pd
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse


class GeniusScraper:
    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date
        self.dates_links = []
        self.unique_lyrics_links = {}
        self.session = self.setting_connection()


    @staticmethod
    def get_link(date: datetime):
        date_str = date.strftime('%Y%m%d')
        webarchive = f"http://web.archive.org/web/{date_str}/https://genius.com/"
        return webarchive

    @staticmethod
    def log(filename, message:str, page_url):
        logfile = f"{filename}.txt"
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open(logfile, "a") as f:
            f.write(timestamp + ', ' + f'{message}' + ':' + f'{page_url}' + '\n'
                    )

    @staticmethod
    def clear_log(filename):
        logfile = f"{filename}.txt"
        with open(logfile, "w") as f:
            pass


    @staticmethod
    def has_lyrics_class(tag):
        for value in tag.get('class', []):
            if value.startswith(
                    'Lyrics__Container-sc-1ynbvzw-') or value == 'lyrics' \
                    or value == 'lyrics_ctrl.editing' or value == 'song_body-lyrics':
                return True
        return False


    @staticmethod
    def setting_connection():
        session = requests.Session()

        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        return session


    def collect_lyrics(self, lyrics_link, lyrics_list):
        unique_linkpart = self.unique_bit(lyrics_link)
        if unique_linkpart in self.unique_lyrics_links.values():
            lyrics_list.append('We already listened to this one')
            pass
        else:
            self.log('songs_log', 'collecting lyrics', lyrics_link)
            self.unique_lyrics_links[unique_linkpart] = unique_linkpart
            r = self.setting_connection().get(lyrics_link)
            soup = BeautifulSoup(r.content, "html.parser")
            lyrics_list.append(soup.find(self.has_lyrics_class).get_text())
            time.sleep(random.randint(2, 4))
        return lyrics_list


    def parser_2018(self, soup, output):
        """
        works until 2019 6 3, after that need to switch to another parser
        """
        lyrics_list = []

        rank = soup.find_all("div", ["chart_row-number_container chart_row-number_container--large"])
        rank = re.findall('(?<=<span\>)(.*?)(?=</span>)', str(rank))

        song_name_3 = soup.find_all("div", ["chart_row-two_line_title_and_artist-title"])
        song_name_separate_3 = re.findall('(?<="\>)(.*?)(?=</div>)', str(song_name_3))

        song_name_7 = soup.find_all("div", ["chart_row-content"])
        song_name_separate_7 = re.findall('(?<="\>)(.*?)(?= by)', str(song_name_7))

        artist_name_3 = soup.find_all("div", ["chart_row-two_line_title_and_artist"])
        artist_name_separate_3 = re.findall('(?<=      )(.*?)(?=\n)', str(artist_name_3))

        artist_name_7 = soup.find_all("div", ["chart_row-content"])
        artist_name_separate_7 = re.findall('(?<=by )(.*?)(?=</div>)', str(artist_name_7))

        lyrics_links = soup.find_all("a", ["chart_row"])
        lyrics_links = re.findall('(?<=href=")(.*?)(?=">)', str(lyrics_links))

        for lyrics_link in lyrics_links:
            lyrics_list = self.collect_lyrics(lyrics_link, lyrics_list)


        output.append({
            'ranks': rank,
            'song_titles': song_name_separate_3 + song_name_separate_7,
            'artists': artist_name_separate_3 + artist_name_separate_7,
            'lyrics_links': lyrics_links,
            'lyrics': lyrics_list
        })



    @staticmethod
    def parser_2019(soup, output):
        rank = soup.find_all("div", ["ChartItemdesktop__Rank-sc-3bmioe-1"])
        rank = re.findall('(?<="\>)(.*?)(?=</div>)', str(rank))

        song_name = soup.find_all("div", ["ChartSongdesktop__Title-sc-18658hh-3"])
        song_name = re.findall('(?<="\>)(.*?)(?=</div>)', str(song_name))

        artist_name = soup.find_all("h4", ["ChartSongdesktop__Artist-sc-18658hh-5"])
        artist_name = re.findall('(?<="\>)(.*?)(?=</h4>)', str(artist_name))

        lyrics = soup.find_all("a", ["PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 izVEsw",
                                        "PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 qsIlk"])
        lyrics_links = re.findall('(?<=href=")(.*?)(?=">)', str(lyrics))

        output.append({"ranks": rank,
                     "song_titles": song_name,
                     "artists": artist_name,
                     "lyrics_links": lyrics_links})


    @staticmethod
    def unique_bit(link):
        parsed_url = urlparse(link)
        path_parts = parsed_url.path.split('/')
        return path_parts[6]


    def generate_links(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            self.dates_links.append([current_date, self.get_link(current_date)])
            current_date += timedelta(days=1)


    def collect_data(self):
        self.generate_links()
        output = []
        self.clear_log('songs_log')

        for date, link in self.dates_links:
            time.sleep(random.randint(2, 4))

            self.log('songs_log', 'collecting song data', link)

            r = self.setting_connection().get(link)
            soup = BeautifulSoup(r.content, "html.parser")

            if date > datetime(2019, 6, 3):
                self.parser_2019(soup, output)
            else:
                self.parser_2018(soup, output)


        song_info_df = pd.DataFrame(output)
        song_info_df = song_info_df.explode(['ranks',
                                             'song_titles',
                                             'artists',
                                             'lyrics_links',
                                             'lyrics']).reset_index(drop=True)
        song_info_df = song_info_df[song_info_df.lyrics != 'We already listened to this one']
        return song_info_df


start_date = datetime(2018, 1, 1)
end_date = datetime(2018, 3, 1)
scraper = GeniusScraper(start_date, end_date)
song_info_df = scraper.collect_data()
