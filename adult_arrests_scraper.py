import pandas as pd
import numpy as np
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

project_folder = '/home/hackerlikecomputer/CPD-Adult-Arrests-Scraper'
load_dotenv(os.path.join(project_folder, '.env'))
account_sid = os.getenv('TWILIO_SID')
auth_token = os.getenv('TWILIO_AUTH')
client = Client(account_sid, auth_token)

def clear_output():
    os.system('cls' if os.name == 'nt' else 'clear')

def is_gun_crime(charge):
    if 'UUW' in charge:
        return True
    elif 'GUN' in charge:
        return True
    elif 'FIREARM' in charge:
        return True
    elif 'ARMED HABITUAL CRIMINAL' in charge:
        return True
    else:
        return False

def is_violent_crime(charge):
    if 'HOMICIDE' in charge:
        return True
    elif 'MURDER' in charge:
        return True
    elif 'ROBBERY' in charge:
        return True
    elif 'AGGRAVATED ASSAULT' in charge or 'AGG AS' in charge:
        return True
    elif 'AGGRAVATED BATTERY' in charge or 'AGG BAT' in charge:
        return True
    elif 'SEXUAL ASSAULT' in charge or 'SEX AS' in charge or 'CSA' in charge:
        return True
    else:
        return False

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
            notify(f'Request has timed out {retry_counter} times. Waiting {wait_time} before continuing.')
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
        raise Timeout(f'Request failed in line {getframeinfo(currentframe()).lineno} after {retry_counter} attempts, waited a total of {total_wait_time/60} minutes')

#getting list of all beats from CPD shapefile data
beats = pd.read_csv('https://data.cityofchicago.org/api/views/n9it-hstw/rows.csv?accessType=DOWNLOAD')
#beat nos are always 4 digits, zero-padded from the left
beats['BEAT_NUM'] = beats.BEAT_NUM.astype('str').str.zfill(4)
#list to loop through
beats = beats.BEAT_NUM.unique().tolist()

datasets = []
#loop through each district
for beat in tqdm(beats, position=0, desc='BEATS'):
    results = []
    url = f'http://publicsearch3.azurewebsites.net/Arrests?Page=1&Beat={beat}&IsEmpty=True'
    #get number of pages possible
    #allow retries
    search_results = get_soup_with_retry(url)
    #check for additional page buttons
    #get the last possible page number to catch all pages
    last_page_btn = search_results.find('li', {'class':'PagedList-skipToLast'})
    if last_page_btn is not None:
        last_page_btn = last_page_btn.find('a')['href']
        max_page_no = int(re.findall(r'Page=(\d+)', last_page_btn)[0]) + 1
    elif last_page_btn is None:
        #if no buttons, max_page_no is just 2 so it hits only one page
        max_page_no = 2
    else:
        raise ScraperError(f'Scraper failed at line {getframeinfo(currentframe()).lineno}: Unable to assign max_page_no')
    for page in tqdm(range(1, max_page_no), position=1, desc='RESULTS PAGES'):
        page_url = f'http://publicsearch3.azurewebsites.net/Arrests?Page={page}&Beat={beat}&IsEmpty=True'
        results_page = get_soup_with_retry(page_url)
        buttons = results_page.findAll('a', {'class', 'btn-default'})
        if len(buttons) == 0:
            raise ScraperError(f'Scraper failed at line {getframeinfo(currentframe()).lineno}, unable to locate detail page buttons')
        for button in tqdm(buttons, position=2, desc='BUTTONS'):
            url_start = 'http://publicsearch3.azurewebsites.net'
            detail_page_url = url_start + button['href']
            results.append(detail_page_url)

    datasets.append((beat, results))
    #make an archive of datasets in case it breaks
    with open("/home/hackerlikecomputer/CPD-Adult-Arrests-Scraper/archive/datasets.txt", "w+") as f:
        pickle.dump(datasets, f)

    notify(f'Scraper successfully retrieved URLs from {len(results)} offenders in beat {beat}')

#create a separate file for each district rather than one massive file
#data structure is [(beat1:[url1, url2, url3]), (beat2: [url1, url2, url3])]
#each dataset is a beat
for dataset in tqdm(datasets, position=0, desc='DATASETS'):
    beat = str(dataset[0])

    statutes = []
    charge_descs = []
    names = []
    ages = []
    scraped_cbs = []
    arrest_dates = []
    arrest_locs = []
    arrest_agencys = []
    bond_types = []
    bond_amounts = []
    bond_dates = []
    areas = []
    districts = []
    beats = []

    for link in tqdm(dataset[1], position=1, desc='PAGES'):
        page = get_soup_with_retry(link)
        try:
            table = page.find('table')
        except:
            continue
        rows = table.findAll('tr')
        for tr in tqdm(rows, position=2, desc='TABLE'):
            try:
                dls = page.findAll('dl', {'class':'dl-horizontal'})
            except:
                dls = np.NaN
            try:
                statute = tr.findAll('td')[0].text.strip()
            except:
                statute = np.NaN
            try:
                charge_desc = tr.findAll('td')[1].text.strip()
            except:
                charge_desc = np.NaN
            #the rest of these aren't in the table
            #they need to be appended to the list for each charge, however
            try:
                name = dls[0].findAll('dd')[0].text.strip()
            except:
                name = np.NaN
            try:
                age = dls[0].findAll('dd')[1].text.strip()
            except:
                age = np.NaN
            try:
                scraped_cb = dls[0].findAll('dd')[2].text.strip()
            except:
                scraped_cb = np.NaN
            try:
                arrest_date = dls[1].findAll('dd')[0].text.strip()
            except:
                arrest_date = np.NaN
            try:
                arrest_loc = dls[1].findAll('dd')[1].text.strip()
            except:
                arrest_loc = np.NaN
            try:
                arrest_agency = dls[1].findAll('dd')[2] .text.strip()
            except:
                arrest_agency = np.NaN
            try:
                bond_type = dls[2].findAll('dd')[0].text
            except:
                bond_type = np.NaN
            try:
                bond_amount = dls[2].findAll('dd')[1].text.strip()
            except:
                bond_amount = np.NaN
            try:
                bond_date = dls[2].findAll('dd')[2].text.strip()
            except:
                bond_date = np.NaN
            try:
                area = dls[3].findAll('dd')[0].text.strip()
            except:
                area = np.NaN
            try:
                district = dls[3].findAll('dd')[1].text.strip()
            except:
                district = np.NaN

            statutes.append(statute)
            charge_descs.append(charge_desc)
            names.append(name)
            ages.append(age)
            scraped_cbs.append(scraped_cb)
            arrest_dates.append(arrest_date)
            arrest_locs.append(arrest_loc)
            arrest_agencys.append(arrest_agency)
            bond_types.append(bond_type)
            bond_amounts.append(bond_amount)
            bond_dates.append(bond_date)
            areas.append(area)
            districts.append(district)
            beats.append(beat)
    try:
        df = pd.DataFrame({
            'statute':statutes,
            'charge_desc':charge_descs,
            'name':names,
            'age':ages,
            'cb':scraped_cbs,
            'arrest_date':arrest_dates,
            'arrest_loc':arrest_locs,
            'arrest_agency':arrest_agencys,
            'bond_type':bond_types,
            'bond_amount':bond_amounts,
            'bond_date':bond_dates,
            'area':areas,
            'district':districts,
            'beat':beats,
        })
    except:
        raise ScraperError(f'Scraper failed at line {getframeinfo(currentframe()).lineno}. Unable to construct DataFrame from beat {beat}')

    #ensure too much data isn't NaN
    #cb number category is mandatory
    if df.name.isna().all() is False and df.cb.isna().all():
        raise ScraperError(f'Scraper failed at line {getframeinfo(currentframe()).lineno}. Entire name and cb no columns are null.')

    #add crime type categories
    df['gun_crime'] = df.charge_desc.astype(str).apply(is_gun_crime)
    df['violent_crime'] = df.charge_desc.astype(str).apply(is_violent_crime)
    df['sex_crime'] = df.charge_desc.astype(str).apply(lambda charge: True if 'SEX' in charge else False)
    df['domestic'] = df.charge_desc.astype(str).apply(lambda charge: True if 'DOMESTIC' in charge else False)

    #clean up whitespace
    df['arrest_loc'] = df.arrest_loc.astype(str).str.replace('\r', '').str.replace('\t', '').str.replace('\n', ' ')
    df['bond_type'] = df.bond_type.astype(str).str.replace('\r', '').str.replace('\t', '').str.replace('\n', ' ')
    df['area'] = df.area.astype(str).str.replace('\r', '').str.replace('\t', '').str.replace('\n', ' ')
    df['bond_type'] = [np.NaN if bond == ' ' else bond for bond in df.bond_type]
    df['bond_type'] = df.bond_type.str.replace('[A-z]+', '', regex=True)

    #directory to save file in
    local_path = '/home/hackerlikecomputer/CPD-Adult-Arrests-Scraper/data/'
    #save df as csv locally
    #then upload the files as a new table in the database
    df.to_csv(f'{local_path}beat_{beat}.csv', index=False)
    notify(f'Success! Scraped {len(df)} records from beat {beat} and saved to file at {local_path}beat_{beat}.csv')
