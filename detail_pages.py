import pandas as pd
import requests
from bs4 import BeautifulSoup
from twilio.rest import Client
import re
import time
import os
import pickle
from dotenv import load_dotenv
from inspect import currentframe, getframeinfo
from tqdm import tqdm

project_folder = os.getcwd()
load_dotenv(os.path.join(project_folder, '.env')) 
account_sid = os.getenv('TWILIO_SID')
auth_token = os.getenv('TWILIO_AUTH')
client = Client(account_sid, auth_token)

def clear_output():
    os.system('cls' if os.name == 'nt' else 'clear')


def notify(message):
    message = client.messages \
                    .create(
                         body=message,
                         from_='+13178303432',
                         to='+13179897004'
                     )


class ScraperError(Exception):
    def __init__(self, message):
        self.message = message
        notify(self.message)


class Timeout(Exception):
    def __init__(self, message):
        self.message = message
        notify(self.message)


def get_soup_with_retry(url):
    retry_counter = 0
    wait_time = retry_counter ** 2 * 10
    total_wait_time = 0
    while retry_counter <= 10:
        if retry_counter > 0:
            msg = f'Request has timed out {retry_counter} times. \
                    Waiting {wait_time} before continuing.'
            notify(msg)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return BeautifulSoup(r.content, "html.parser")
        except requests.exceptions.Timeout:
            retry_counter += 1
            total_wait_time += wait_time
            time.sleep(wait_time)
            continue
    else:
        line = getframeinfo(currentframe()).lineno + 1
        msg = f'Request failed in line {line} after {retry_counter} attempts, \
                waited a total of {total_wait_time/60} minutes'
        raise Timeout(msg)

# getting list of all beats from CPD shapefile data
beats = pd.read_csv('https://data.cityofchicago.org/api/views/\
                     n9it-hstw/rows.csv?accessType=DOWNLOAD')
# beat nos are always 4 digits, zero-padded from the left
beats['BEAT_NUM'] = beats.BEAT_NUM.astype('str').str.zfill(4)
# list to loop through
beats = beats.BEAT_NUM.unique().tolist()

datasets = []
# loop through each district
for beat in tqdm(beats, position=0, desc='BEATS'):
    results = []
    url = f'http://publicsearch3.azurewebsites.net/Arrests\
            ?Page=1&Beat={beat}&IsEmpty=True'
    # get number of pages possible
    # allow retries
    search_results = get_soup_with_retry(url)
    # check for additional page buttons
    # get the last possible page number to catch all pages
    b = 'li', {'class': 'PagedList-skipToLast'}
    last_page_btn = search_results.find(b)
    if last_page_btn is not None:
        last_page_btn = last_page_btn.find('a')['href']
        max_page_no = int(re.findall(r'Page=(\d+)', last_page_btn)[0]) + 1
    elif last_page_btn is None:
        # if no buttons, max_page_no is just 2 so it hits only one page
        max_page_no = 2
    else:
        line = getframeinfo(currentframe()).lineno
        msg = f'Scraper failed at line {line + 1}: Unable to assign \
                max_page_no'
        raise ScraperError(msg)
    for page in tqdm(range(1, max_page_no), position=1, desc='RESULTS PAGES'):
        page_url = f'http://publicsearch3.azurewebsites.net/Arrests\
                     ?Page={page}&Beat={beat}&IsEmpty=True'
        results_page = get_soup_with_retry(page_url)
        buttons = results_page.findAll('a', {'class', 'btn-default'})
        if len(buttons) == 0:
            line = getframeinfo(currentframe()).lineno
            msg = f'Scraper failed at line {line + 1}, \
                    unable to locate detail page buttons'
            raise ScraperError(msg)
        for button in tqdm(buttons, position=2, desc='BUTTONS'):
            url_start = 'http://publicsearch3.azurewebsites.net'
            detail_page_url = url_start + button['href']
            results.append(detail_page_url)

    datasets.append((beat, results))
    # make an archive of datasets in case it breaks
    with open(f"{project_folder}/archive/datasets.txt", "wb+") as f:
        pickle.dump(datasets, f)

    notify(f'Scraper successfully retrieved URLs from {len(results)} \
             offenders in beat {beat}')


#%%
