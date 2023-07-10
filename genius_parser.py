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
import json
import logging


class GeniusScraper:
    def __init__(self, config_path: str, start_date: datetime, end_date: datetime):
        logging.basicConfig(
            filename='song_parsing.log',
            filemode='w',
            format='%(asctime)s: %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=logging.INFO
        )
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


    def log(self, message:str, page_url):
        if 'song data' in message:
            logging.warning(f'{message}: {page_url}')
        else:
            logging.info(f'{message}: {page_url}')


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

        retry = Retry(connect=5, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        return session


    def collect_lyrics(self, lyrics_link, lyrics_list):
        unique_linkpart = self.unique_bit(lyrics_link)
        if unique_linkpart in self.unique_lyrics_links.values():
            lyrics_list.append('We already listened to this one')
        else:
            self.log('collecting lyrics', lyrics_link)
            self.unique_lyrics_links[unique_linkpart] = unique_linkpart
            r = self.set_connection().get(lyrics_link)
            soup = BeautifulSoup(r.content, "html.parser")


            try:
                lyrics_tag = soup.find(self.has_lyrics_class)
                lyrics_list.append(lyrics_tag.get_text())
            except AttributeError:
                print('No lyrics found')
                # lyrics_list.append('No lyrics found')

        return lyrics_list


    def get_element_content(self, soup, tag:str, element_class:list, str_before:str, str_after:str):
        element_content = soup.find_all(tag, element_class)
        return re.findall(f'(?<={str_before})(.*?)(?={str_after})', str(element_content))


    def parse_web_page_old_version(self, soup, date):
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
            time.sleep(random.randint(3, 4))

        return {
            'date': date,
            'ranks': rank,
            'song_titles': song_name,
            'artists': artist_name,
            'lyrics_links': lyrics_links,
            'lyrics': lyrics_list
            }

    def parse_web_page_new_version(self, soup, date):
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
            time.sleep(random.randint(3, 4))

        return {
            'date': date,
            'ranks': rank,
            'song_titles': song_name,
            'artists': artist_name,
            'lyrics_links': lyrics_links,
            'lyrics': lyrics_list
        }


    def parse_lyrics(self, soup, output, date):
        if date < datetime(2019, 6, 3):
            data = self.parse_web_page_old_version(soup, date)
        else:
            data = self.parse_web_page_new_version(soup, date)
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
            output = pd.read_csv('filename.csv', index_col=0)
            iteration = self.config['iteration']
        else:
            output = pd.DataFrame()
            iteration = 0

        for date, link in self.dates_links:

            self.log(f'collecting song data from that date {date.strftime(("%Y-%m-%d"))}', link)

            try:
                r = self.set_connection().get(link)

                soup = BeautifulSoup(r.content, "html.parser")

                output = self.parse_lyrics(soup, output, date)
                iteration += 1

            except ValueError as e:
                self.log(f"Error occurred while processing link {link}: {str(e)}", link)
                iteration += 1
                continue

            except Exception as e:
                self.log(f"Unexpected error occurred while processing link {link}: {str(e)}", link)
                self.save_config(iteration)
                output.to_csv('filename.csv')
                break


        song_info_df = output[output['lyrics'] != 'We already listened to this one']
        return song_info_df

start_date = datetime(2020, 1, 3)
end_date = datetime(2023, 7, 8)
scraper = GeniusScraper('./config.json', start_date, end_date)
song_info_df = scraper.collect_data()

df = pd.read_csv('filename.csv')