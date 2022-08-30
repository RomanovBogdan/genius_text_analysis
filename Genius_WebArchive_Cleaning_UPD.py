import pandas as pd
import re
import nltk
from nltk.corpus import stopwords

df = pd.read_excel('title_data_lyrics_2018_2022.xlsx')

lyrics = df['lyrics'].tolist()
lyrics = [i.lower() for i in lyrics]
lyrics = [re.sub("\[.*\]", " ", i) for i in lyrics]
lyrics = [re.sub("'", "", i) for i in lyrics]
lyrics = [re.sub("\W", " ", i) for i in lyrics]
lyrics = [re.sub("\s+", " ", i) for i in lyrics]
df['new_cleaning'] = lyrics

stop = set(stopwords.words('english'))
new_stopwords = ['dont', 'im', 'ive', 'youre', 'id', 'didnt', 'wouldve', 'aint']
new_stopwords_list = stop.union(new_stopwords)
df['lyrics_without_stopwords'] = df['new_cleaning'].apply(lambda x: ' '.join([word for word in x.split() if word not in (new_stopwords_list)]))
df = df.drop('clean_lyrics', axis=1)
df.to_excel('lyrics_20182022.xlsx')