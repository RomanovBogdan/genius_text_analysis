import random
import pandas as pd
from time import sleep
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import json


class GeniusScraper:
    def __init__(self, config_path: str, start_date: datetime, end_date: datetime):
        self.config_path = config_path
        self.start_date = start_date
        self.end_date = end_date
        self.dates_links = []
        self.unique_lyrics_links = {}
        self.session = self.set_connection()
        self.lyrics_list = []
        self.load_config()



    @staticmethod
    def get_link(date: datetime):
        date_str = date.strftime('%Y%m%d')
        return f"http://web.archive.org/web/{date_str}/https://genius.com/"


    @staticmethod
    def log(filename, message:str, page_url):
        logfile = f"{filename}.txt"
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open(logfile, "a") as f:
            f.write(timestamp + ', ' + f'{message}' + ': ' + f'{page_url}' + '\n')


    @staticmethod
    def clear_log(filename):
        logfile = f"{filename}.txt"
        with open(logfile, "w"):
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
    def set_connection():
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
        else:
            self.log('songs_log', 'collecting lyrics', lyrics_link)
            self.unique_lyrics_links[unique_linkpart] = unique_linkpart
            r = self.set_connection().get(lyrics_link)
            soup = BeautifulSoup(r.content, "html.parser")

            lyrics_list.append(soup.find(self.has_lyrics_class).get_text())

            sleep(random.randint(2, 4))
        return lyrics_list


    def get_element_content(self, soup, tag:str, element_class:list, str_before:str, str_after:str):
        element_content = soup.find_all(tag, element_class)
        return re.findall(f'(?<={str_before})(.*?)(?={str_after})', str(element_content))


    def parse_web_page_old_version(self, soup):
        rank = self.get_element_content(soup, 'div', ["chart_row-number_container chart_row-number_container--large"],
                                     '<span\>', '</span>')
        song_name_3 = self.get_element_content(soup, 'div', ["chart_row-two_line_title_and_artist-title"],
                                                   '"\>', '</div>')
        song_name_7 = self.get_element_content(soup, 'div', ["chart_row-content"],
                                                   '"\>', ' by')
        song_name = song_name_3 + song_name_7
        artist_name_3 = self.get_element_content(soup, 'div', ["chart_row-two_line_title_and_artist"],
                                                   '      ', '\n')
        artist_name_7 = self.get_element_content(soup, 'div', ["chart_row-content"],
                                                   'by ', '</div>')
        artist_name = artist_name_3 + artist_name_7
        lyrics_links = self.get_element_content(soup, 'a', ['chart_row'], 'href="', '">')

        lyrics_list = []
        for lyrics_link in lyrics_links:
            lyrics_list = self.collect_lyrics(lyrics_link, lyrics_list)
        return {
            'ranks': rank,
            'song_titles': song_name,
            'artists': artist_name,
            'lyrics_links': lyrics_links,
            'lyrics': lyrics_list
            }

    def parse_web_page_new_version(self, soup):
        rank = self.get_element_content(soup, "div", ["ChartItemdesktop__Rank-sc-3bmioe-1"],
                                        '"\>', '</div>')
        song_name = self.get_element_content(soup, "div", ["ChartSongdesktop__Title-sc-18658hh-3"],
                                             '"\>', '</div>')
        artist_name = self.get_element_content(soup, "h4", ["ChartSongdesktop__Artist-sc-18658hh-5"],
                                               '"\>', '</h4>')
        lyrics_links = self.get_element_content(soup, "a",
                                                ["PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 izVEsw",
                                                 "PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 qsIlk"],
                                                'href="', '">')
        lyrics_list = []
        for lyrics_link in lyrics_links:
            lyrics_list = self.collect_lyrics(lyrics_link, lyrics_list)

        return {
            'ranks': rank,
            'song_titles': song_name,
            'artists': artist_name,
            'lyrics_links': lyrics_links,
            'lyrics': lyrics_list
        }


    def parse_lyrics(self, soup, output, date):
        if date <= datetime(2019, 6, 3):
            print('Using old version')
            data = self.parse_web_page_old_version(soup)
        else:
            print('Using new version')
            data = self.parse_web_page_new_version(soup)
        output = pd.concat([output, pd.DataFrame(data)], ignore_index=True)
        return output

    @staticmethod
    def unique_bit(link):
        return urlparse(link).path.split('/')[6]


    def generate_links(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            self.dates_links.append([current_date, self.get_link(current_date)])
            current_date += timedelta(days=1)

    def load_config(self):
        with open(self.config_path) as cfg:
            self.config = json.load(cfg)

    def save_config(self, iteration):
        self.config['iteration'] = iteration
        with open('./config.json', 'w', encoding='utf-8') as f:
            json.dump(self.config, f, ensure_ascii=False, indent=4)


    def collect_data(self):
        self.generate_links()

        if self.config['iteration'] != 0:
            output = pd.read_csv('filename.csv')
            iteration = self.config['iteration']
        else:
            output = pd.DataFrame()
            iteration = 0

        self.clear_log('songs_log')

        try:
            for date, link in self.dates_links:
                sleep(random.randint(2, 4))

                self.log('songs_log', 'collecting song data', link)

                r = self.set_connection().get(link)
                soup = BeautifulSoup(r.content, "html.parser")

                output = self.parse_lyrics(soup, output, date)
            iteration += 1

        except Exception:
            self.save_config(iteration)
            output.to_csv('filename.csv')
        song_info_df = output[output.lyrics != 'We already listened to this one']
        return song_info_df

start_date = datetime(2019, 6, 1)
end_date = datetime(2019, 6, 5)
scraper = GeniusScraper('./config.json', start_date, end_date)
song_info_df = scraper.collect_data()