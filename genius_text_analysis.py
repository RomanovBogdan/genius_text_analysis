import pandas as pd
import string
import re
from langdetect import detect


def load_data(file_path):
    df = pd.read_csv(file_path, index_col=0)
    df = df[df['lyrics'] != 'We already listened to this one'].drop_duplicates()
    return df

def clean_text(text):
    # Remove all characters in square brackets
    text = re.sub(r'\[.*?\]', '', text)

    # Replace newline characters with space
    text = text.replace('\n', ' ')
    text = text.replace('\u200b', ' ')
    text = text.replace('&amp;', 'and')

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = ' '.join(text.split())
    return text.lower()

def prepare_strings(df):
    df['song_titles'] = [clean_text(title) for title in df['song_titles']]
    df['artists'] = [clean_text(artist) for artist in df['artists']]
    df['clean_text'] = [clean_text(song) for song in df['lyrics']]
    df['language'] = [detect(text) for text in df['lyrics']]
    return df


df = load_data('filename.csv')
df_clean = prepare_strings(df)
df_clean = df_clean.query('language == "en"')