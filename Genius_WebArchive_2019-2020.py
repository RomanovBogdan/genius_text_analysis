import pandas as pd
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm
import datetime
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from langdetect import detect
import plotly.express as px
import plotly.graph_objects as go

### Loop for collecting data from the title pages

today = datetime.today()
start_date_formated = datetime.strptime("2019 06 04", '%Y %m %d')
new_start_date = start_date_formated + timedelta(days=300)
numbdays = today - start_date_formated


data = []
for j in range(210):
    date = [start_date_formated + timedelta(days=j) for j in range(210)]
    webarchive = f"http://web.archive.org/web/{date[j].strftime('%Y%m%d')}/https://genius.com/"
    # for i in tqdm(range(10)):
    #     sleep(0.5)
    for position in range(10):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        page = session.get(webarchive)

        if page.status_code != 200:
            continue
        try:
            html = BeautifulSoup(page.content, "html.parser")

            rank = html.find_all("div", ["ChartItemdesktop__Rank-sc-3bmioe-1"])
            rank_separate = re.findall('(?<="\>)(.*?)(?=</div>)', str(rank))
            # rank_separate = re.findall('(?<="ChartItemdesktop__Rank-sc-3bmioe-1 tDViA">)(.*?)(?=</div>)', str(rank))

            song_name = html.find_all("div", ["ChartSongdesktop__Title-sc-18658hh-3"])
            song_name_separate = re.findall('(?<="\>)(.*?)(?=</div>)', str(song_name))
            # song_name_separate = re.findall('(?<=ChartSongdesktop__Title-sc-18658hh-3 fODYHn">)(.*?)(?=</div>)', str(song_name))

            artist_name = html.find_all("h4", ["ChartSongdesktop__Artist-sc-18658hh-5"])
            artist_name_separate = re.findall('(?<="\>)(.*?)(?=</h4>)', str(artist_name))
            # artist_name_separate = re.findall('(?<=ChartSongdesktop__Artist-sc-18658hh-5 kiggdb">)(.*?)(?=</h4>)', str(artist_name))

            all_links = html.find_all("a", ["PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 izVEsw",
                                            "PageGriddesktop-a6v82w-0 ChartItemdesktop__Row-sc-3bmioe-0 qsIlk"])
            lyrics_URL = re.findall('(?<=href=")(.*?)(?=">)', str(all_links))

            data.append({"Date": date[j],
                         "Rank": rank_separate[position],
                         "Song name": song_name_separate[position],
                         "Artist name": artist_name_separate[position],
                         "Lyrics URL": lyrics_URL[position]})
        except:
            pass

    print(j)
data = pd.DataFrame(data)
print(data)
data.to_excel(f"title_data_{(j+1)*10}_20119_2020.xlsx")

### Collecting lyrics
pre_results = pd.read_excel(f"title_data_{(j+1)*10}_20119_2020.xlsx")
pre_results.sort_values("Song name", inplace = True)
pre_results.drop_duplicates(subset = "Song name", keep = "first", inplace = True)

lyrics_URL_only = pre_results['Lyrics URL']
lyrics_URL_only = lyrics_URL_only.reset_index(drop=True)

tmp_lyrics = []

for j in range(0, len(lyrics_URL_only)):
    one_link = lyrics_URL_only[j] ### 2567 data has no lyrics uploaded
    page = requests.get(one_link)
    if page.status_code != 200:
        continue

    html = BeautifulSoup(page.content, "html.parser")
    lyrics = html.find("div", ["lyrics", "Lyrics__Container-sc-1ynbvzw-8",
                               "Lyrics__Container-sc-1ynbvzw-6",
                               "Lyrics__Container-sc-1ynbvzw-10",
                               "Lyrics__Container-sc-1ynbvzw-7",
                               "Lyrics__Container-sc-1ynbvzw-2",
                               "LyricsPlaceholder__Message-uen8er-3",
                               "LyricsPlaceholder__Message-uen8er-4"]).get_text()

                               # "Lyrics__Container-sc-1ynbvzw-6 krDVEH",
                               # "Lyrics__Container-sc-1ynbvzw-10 cvsIWi",
                               # "Lyrics__Container-sc-1ynbvzw-8 beMmeb"]).get_text()
    tmp_lyrics.append({"Link": lyrics_URL_only[j],
                      "Lyrics": lyrics})

    print(j)
    sleep(0.5)

lyrics_df = pd.DataFrame(tmp_lyrics)
print(lyrics_df)
lyrics_df.to_excel(f"lyrics_{j+1}_sorted_2019_2020.xlsx")
lyrics_df.columns = ["Lyrics URL", "Lyrics"]

results = pd.merge(data, lyrics_df, on="Lyrics URL")
results.to_excel(f"title_lyrics_data_{j}_2018_2020.xlsx")


### deleting duplicates from the dataset
results = pd.read_excel("title_lyrics_data.xlsx")
results.sort_values("Song name", inplace = True)
results.drop_duplicates(subset = "Song name", keep = "first", inplace = True)

### combining two DFs for 800 days
title_800 = pd.read_excel("title_data_800.xlsx")
lyrics_800 = pd.read_excel("lyrics_800_sorted.xlsx")
lyrics_800 = lyrics_800.drop("Unnamed: 0", axis = 1)
lyrics_800.columns = ['Lyrics URL', 'Lyrics']

final_df = pd.merge(title_800, lyrics_800, on="Lyrics URL") # DF with combined title data and lyrics for 800 days

### Cleaning the lyrics

lyrics = final_df['Lyrics'].tolist()

lyrics = [i.lower() for i in lyrics]
lyrics = [i.replace("\n\n", " ") for i in lyrics]
lyrics = [i.replace("\n", " ") for i in lyrics]
lyrics = [i.replace('\u200a', "")for i in lyrics]
lyrics = [i.replace('\u2005', " ")for i in lyrics]
lyrics = [i.replace("\\'", "'") for i in lyrics]
lyrics = [i.replace('  ', "") for i in lyrics]
lyrics = [i.replace('   ', "") for i in lyrics]

lyrics = [i.replace('[verse 1] ', " ") for i in lyrics]
lyrics = [i.replace('[verse 1] ', " ") for i in lyrics]
lyrics = [i.replace('[verse 1]', " ") for i in lyrics]
lyrics = [i.replace('[verse 2]', " ") for i in lyrics]
lyrics = [i.replace('[verse 3]', " ") for i in lyrics]
lyrics = [i.replace('[verse 4]', " ") for i in lyrics]
lyrics = [i.replace('[bridge]', " ") for i in lyrics]
lyrics = [i.replace('[verse]', " ") for i in lyrics]
lyrics = [i.replace('[verse] ', " ") for i in lyrics]
lyrics = [i.replace('[chorus] ', " ") for i in lyrics]
lyrics = [i.replace('[chorus]', " ") for i in lyrics]
lyrics = [i.replace('[post-chorus] ', " ") for i in lyrics]
lyrics = [i.replace('[intro] ', " ") for i in lyrics]
lyrics = [i.replace('[intro]', " ") for i in lyrics]
lyrics = [i.replace('[outro] ', " ") for i in lyrics]
lyrics = [i.replace('[part i] ', " ") for i in lyrics]
lyrics = [i.replace('[pre-chorus] ', " ") for i in lyrics]

lyrics = [re.sub("\[intro: .*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[verse 1:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[verse 2:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[verse 3:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[verse 4:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[chorus:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[pre-chorus:.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[текст песни .*\]", " ", i) for i in lyrics]
lyrics = [re.sub("\[refren:.*\]", " ", i) for i in lyrics]

final_df['clean_lyrics'] = lyrics
# check the number of instances of english words in all the songs (k-pop and russian songs)

final_df['Lyrics language'] = 'eng'
for i in range(len(final_df['clean_lyrics'])):
    try:
        language = detect(final_df['clean_lyrics'][i])
    except:
        pass
    final_df['Lyrics language'][i] = language

final_df.to_excel('all_data_parser.xlsx')
final_df = pd.read_excel('all_data_parser.xlsx')

final_df.columns

fig = px.histogram(x=final_df['Lyrics language'], opacity=1)
fig.update_xaxes(categoryorder="total descending")
fig.show()

final_df = final_df.drop('Unnamed: 0.1', axis = 1)
final_df = final_df.drop('Unnamed: 0', axis = 1)

sorting = final_df['Lyrics language'] == 'en'
final_df_eng = final_df[sorting]