import random
import pandas as pd
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def get_link(start_date:datetime):
    start_date = start_date.strftime('%Y%m%d')
    webarchive = f"http://web.archive.org/web/{start_date}/https://genius.com/"
    return webarchive


def all_links(start_year, start_month, start_day,
              end_year, end_month, end_day):
    links = []

    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)

    current_date = start_date
    while current_date <= end_date:
        links.append(get_link(current_date))
        current_date += timedelta(days=1)
    return links


def has_chart_row_class(tag):
    for value in tag.get('class', []):
        if value.startswith('chart_row'):
            return True
    return False

def log(page_url):
    logfile = "logfile.txt"
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(logfile, "a") as f:
        f.write(timestamp + ', ' + f'opened the following link {page_url}' + '\n'
                )

def clear_log():
    logfile = "logfile.txt"
    with open(logfile, "w") as f:
        pass

def setting_connection():
    session = requests.Session()

    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def parser_2018(soup, output):
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

    lyrics = soup.find_all("a", ["chart_row"])
    lyrics_links = re.findall('(?<=href=")(.*?)(?=">)', str(lyrics))

    output.append({
        'rank': rank,
        'song_title': song_name_separate_3 + song_name_separate_7,
        'artist': artist_name_separate_3 + artist_name_separate_7,
        'lytics_link': lyrics_links
    })


def collect_data():
    links = all_links(2018, 1, 1, 2018, 1, 15)
    output = []
    clear_log()

    for link in links:
        time.sleep(random.randint(0, 4))

        log(link)

        r = setting_connection().get(link)
        soup = BeautifulSoup(r.content, "html.parser")

        parser_2018(soup, output)

    df = pd.DataFrame(output)
    df = df.explode(['rank',
                     'song_title',
                     'artist',
                     'lytics_link']).reset_index(drop=True)
    return df


df = collect_data()