# Web Scraper for Song Lyrics

This project is a Python-based web scraper that collects song lyrics and related metadata from a popular lyrics website via the Wayback Machine platform. The parser specifically collects the songs, which made it to the Genius top-10 chart, updated on a daily basis. Ideally, it could be expanded to collect more songs from the chart once the pagination nuance is overcome.

## Project Overview

The web scraper visits a specified list of URLs, extracts the song lyrics and related data (song title, artist name, and release date), and saves them into a Pandas dataframe. This dataframe is then written to a CSV file for further processing and analysis. The main parameters are `start_date` and `end_date`.

The main objective of this project is to create a dataset for Natural Language Processing (NLP) tasks, such as sentiment analysis, text generation, or language modeling.


## Getting Started

### Prerequisites

Nothing fancy:
- Python 3
- Libraries: requests, beautifulsoup4, pandas, urllib3
- Stable internet connection


### Installation

1. Clone the repo:
```git clone https://github.com/your_username_/project_name.git```


### Usage
Update the timeframe for which you want to collect the song lyrics. 

To run the web scraper, use the command:

```python scraper.py```

The output will be saved in a CSV file named filename.csv in the same directory. Also, all errors will be caught and added to the logs. 


## Contact
Email - romanovbogdan4@gmail.com

Other contacts are in the profile description.

Project Link: https://github.com/RomanovBogdan/genius_text_analysis
