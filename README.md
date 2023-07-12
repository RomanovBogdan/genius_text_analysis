# Web Scraper for Song Lyrics

This project is a Python-based web scraper that collects song lyrics and related metadata from a popular lyrics website via the Wayback Machine platform. The parser specifically collects the songs, which made it to the Genius top-10 chart, updated on a daily basis. Ideally, it could be expanded to collect more songs from the chart once the pagination nuance is overcome.

## Project Overview

The web scraper visits a specified list of URLs, extracts the song lyrics and related data (song title, artist name, and release date), and saves them into a Pandas dataframe. This dataframe is then written to a CSV file for further processing and analysis. The main parameters are `start_date` and `end_date`.

The main objective of this project is to create a dataset for Natural Language Processing (NLP) tasks, such as sentiment analysis, text generation, or language modeling.


## Getting Started

### Prerequisites

Nothing fancy:
- Python 3
- Libraries: requests, beautifulsoup4, pandas
- Stable internet connection

## Contact
Your Name - romanovbogdan4@gmail.com

Project Link: https://github.com/RomanovBogdan/genius_text_analysis
