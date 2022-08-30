import pandas as pd
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from time import sleep
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from langdetect import detect
import plotly.express as px
import plotly.graph_objects as go

### Loop for collecting data from the title pages
start_date_formated = datetime.strptime("2018 01 01", '%Y %m %d')

#there should be 730 iterations
data = []

for j in range(518):
    date = [start_date_formated + timedelta(days=j) for j in range(720)]
    webarchive = f"http://web.archive.org/web/{date[519].strftime('%Y%m%d')}/https://genius.com/"
    for position in range(10):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        page = session.get(webarchive)

        if page.status_code != 200:
            continue

        html = BeautifulSoup(page.content, "html.parser")

        rank = html.find_all("div", ["chart_row-number_container chart_row-number_container--large"])
        rank_separate = re.findall('(?<=<span\>)(.*?)(?=</span>)', str(rank))

        song_name_3 = html.find_all("div", ["chart_row-two_line_title_and_artist-title"])
        song_name_separate_3 = re.findall('(?<="\>)(.*?)(?=</div>)', str(song_name_3))

        song_name_7 = html.find_all("div", ["chart_row-content"])
        song_name_separate_7 = re.findall('(?<="\>)(.*?)(?= by)', str(song_name_7))

        song_name = song_name_separate_3 + song_name_separate_7
        artist_name_3 = html.find_all("div", ["chart_row-two_line_title_and_artist"])
        artist_name_separate_3 = re.findall('(?<=      )(.*?)(?=\n)', str(artist_name_3))

        artist_name_7 = html.find_all("div", ["chart_row-content"])
        artist_name_separate_7 = re.findall('(?<=by )(.*?)(?=</div>)', str(artist_name_7))
        artist_name = artist_name_separate_3 + artist_name_separate_7

        all_links = html.find_all("a", ["chart_row"])
        lyrics_URL = re.findall('(?<=href=")(.*?)(?=">)', str(all_links))

        data.append({"Date": date[j],
                     "Rank": rank_separate[position],
                     "Song name": song_name[position],
                     "Artist name": artist_name[position],
                     "Lyrics URL": lyrics_URL[position]})
    print(j)
data = pd.DataFrame(data)
print(data)
data.to_excel(f"title_data_{(j+1)*10}_2018_2020.xlsx")

### Collecting lyrics
pre_results = pd.read_excel(f"title_data_{(j+1)*10}_2018_2020.xlsx")
pre_results.sort_values("Song name", inplace = True)
pre_results.drop_duplicates(subset = "Song name", keep = "first", inplace = True)
# since the song names are not consistent, there might be duplicates even after drop function

lyrics_URL_only = pre_results['Lyrics URL']
lyrics_URL_only = lyrics_URL_only.reset_index(drop=True)

tmp_lyrics = []
for j in range(0, len(lyrics_URL_only)):
    one_link = lyrics_URL_only[j]
    page = requests.get(one_link)
    if page.status_code != 200:
        continue
    try:
        html = BeautifulSoup(page.content, "html.parser")
        lyrics = html.find("div", ["lyrics",
                                   "Lyrics__Container-sc-1ynbvzw-6",
                                   "Lyrics__Container-sc-1ynbvzw-2",
                                   "Lyrics__Container-sc-1ynbvzw-4",
                                   "Lyrics__Container-sc-1ynbvzw-8",
                                   "Lyrics__Container-sc-1ynbvzw-10",
                                   "lyrics_ctrl.editing",
                                   "song_body-lyrics"]).get_text()
        tmp_lyrics.append({"Lyrics URL": lyrics_URL_only[j],
                          "Lyrics": lyrics})
    except:
        pass
    print(j)
    sleep(0.5)
lyrics_df = pd.DataFrame(tmp_lyrics)
print(lyrics_df)
lyrics_df.to_excel(f"lyrics_{j+1}_sorted_2018_2020.xlsx")

results = pd.merge(pre_results, lyrics_df, on="Lyrics URL")
results.drop_duplicates(subset = "Lyrics", keep = "first", inplace = True)
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