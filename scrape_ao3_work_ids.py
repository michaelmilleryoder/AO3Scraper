import pandas as pd
from tqdm import tqdm as tqdm
from IPython.display import display
from collections import Counter, defaultdict
import os
import re
from collections import defaultdict
import operator
import subprocess
from multiprocessing import Pool
from bs4 import BeautifulSoup
import urllib.request
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pdb

# I/O
fandom_list_fpath = '/usr2/mamille2/fanfiction-project/ao3_books_lit_selected.tsv'

def call_scraper(command):
    subprocess.call(command, shell=True)

def main():
    with open(fandom_list_fpath, 'r') as f:
        fandom_list = [line.split('\t')[0] for line in f.read().splitlines()]
    print(f"Found {len(fandom_list)} fandoms.")

    # Build list of commands to scrape book-based fandoms on AO3

    already_scraped = [
        'Harry Potter - J. K. Rowling',
        'Percy Jackson and the Olympians - Rick Riordan',
        'Hunger Games Series - All Media Types',
        'Twilight Series - Stephenie Meyer',
    ]

    base_url = 'https://archiveofourown.org/works?utf8=%E2%9C%93&work_search%5Bsort_column%5D=kudos_count&work_search%5Bother_tag_names%5D=&work_search%5Bexcluded_tag_names%5D=&work_search%5Bcrossover%5D=&work_search%5Bcomplete%5D=T&work_search%5Bwords_from%5D=&work_search%5Bwords_to%5D=&work_search%5Bdate_from%5D=&work_search%5Bdate_to%5D=&work_search%5Bquery%5D=&work_search%5Blanguage_id%5D=1&commit=Sort+and+Filter&tag_id={}'
    fandoms = [el for el in fandom_list if not el in already_scraped]
    urls = []
    for f in fandoms:
        fandom_url = f.replace(' ', '+').replace('.', '*d*')
        urls.append(base_url.format(fandom_url))
        
    commands = []
    for f, url in zip(fandoms, urls):
        data_dirpath = '/usr2/mamille2/AO3Scraper/data'
        f_lowered = f.lower().replace('- ', '').replace(" ", "_").replace(':', '').replace('/', '_').replace('.', '')
        fandom_dirpath = os.path.join(data_dirpath, f_lowered)
        if not os.path.exists(fandom_dirpath):
            os.mkdir(fandom_dirpath)
        commands.append(f'python /usr2/mamille2/AO3Scraper/ao3_work_ids.py "{url}" --out_csv {fandom_dirpath}/ids')

    # Execute commands
    print("Scraping fandom work IDs...")
    pool = Pool(10)
    pool.map(call_scraper, tqdm(commands))

if __name__ == '__main__': main()
