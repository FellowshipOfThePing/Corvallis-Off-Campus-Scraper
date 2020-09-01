# Description: Utilities for scraping and organizing data

# TODO: Restore_db_from_local

from selenium.webdriver.chrome.options import Options
from lxml.html import fromstring
from pymongo import MongoClient
from scraping.proxies import *
from selenium import webdriver
from bs4 import BeautifulSoup
from termcolor import colored
import scraping.add_features
import scraping.settings
import datetime
import requests
import pprint
import time
import json
import sys
import os


def print_red(text):
    """
    Print given text to console in red font.
    """
    print(colored(text, 'red'))


def print_green(text):
    """
    Print given text to console in green font.
    """
    print(colored(text, 'green'))


def print_blue(text):
    """
    Print given text to console in blue font.
    """
    print(colored(text, 'blue'))


def print_yellow(text):
    """
    Print given text to console in yellow font.
    """
    print(colored(text, 'yellow'))


def capture_page(url):
    """
    Capture Inner-HTML of page at given URL, return as BS4 object
    """
    # Request html and build BS4 object
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(1)
    page = driver.page_source
    driver.quit()
    return BeautifulSoup(page, 'html.parser')



def find_unit_num(address):
    """
    Parse unit number from address string
    """
    unitNum = None
    if ',' in address:
        address = (address[:address.find(',')]).strip()
    if '#' in address:
        unitNum = (address[address.find('#'):]).strip()
        address = (address[:address.find('#')]).strip()
    elif ' - ' in address:
        unitNum = (address[address.find(' - ')+3:]).strip()
        address = (address[:address.find(' - ')]).strip()
    elif 'Apt' in address:
        unitNum = (address[address.find('Apt'):]).strip()
        address = (address[:address.find('Apt')]).strip()
    if unitNum:
        unitDigits = ''.join([c for c in unitNum if c.isdigit()])
        if unitDigits:
            if unitDigits != '0':
                unitNum = '#' + unitDigits
            else:
                unitNum = None
        else:
            unitNum = '#' + ''.join([c for c in unitNum if c.isalpha()])
    return address, unitNum



def find_prices(full_price):
    """
    Parse price_high and price_low properties from given string.
    """
    price_low = None
    if '-' in full_price:
        full_price = full_price.split('-')
        price_low = int(''.join([x for x in full_price[0] if x.isdigit()]))
        price_high = int(''.join([x for x in full_price[1] if x.isdigit()]))
    else:
        price_high = int(''.join([x for x in full_price if x.isdigit()]))
    return price_low, price_high



def find_bed_bath(beds, baths):
    """
    Parse beds and baths properties from given string.
    """
    beds = ''.join([x for x in beds if x.isdigit()])
    if beds:
        if '-' in beds:
            beds = int(beds[:beds.find('-')])
        else:
            beds = int(beds)
    else:
        beds = None
    baths = ''.join([x for x in baths if not x.isalpha()])
    if baths:
        if '-' in baths:
            baths = float(baths[:baths.find('-')])
        else:
            baths = float(baths)
    else:
        baths = None
    return beds, baths



def create_duplicate_id(unit, unit_id=False):
    """
    Create and return a 'duplicate ID', to be used as a key for the given listing.
    """
    alnum_address = [char for char in unit['address'].upper() if char.isalnum()]
    address_id = ''.join(alnum_address)
    price_id = (str(unit['price_high']) + 'P') if unit['price_high'] else ''
    beds_id = (str(unit['beds']) + 'BD') if unit['beds'] else ''
    baths_id = (str(unit['baths']) + 'BA') if unit['baths'] else ''
    sqft_id = (str(unit['sqft']) + 'SQ') if unit['sqft'] else ''
    if unit_id:
        unit_id = unit['unitNum']
        if unit_id:
            unit_id = (str(unit_id).upper()).replace(' ', '')
        features = [address_id, price_id, beds_id, baths_id, sqft_id, unit_id]
    else:
        features = [address_id, price_id, beds_id, baths_id, sqft_id]
    duplicate_id = '-'.join([f for f in features if f])
    return duplicate_id



def backup_collection_to_local(database):
    """
    Copy remote collection and append to local archive file.
    """
    # Connect to DB
    collection = db['listings']

    # Copy documents to local dictionary (creating duplicate keys)
    local_docs = dict()
    print('Pulling collection from DB...')
    documents = collection.find()
    for db_document in documents:
        local_doc = {key:db_document[key] for key in db_document}
        dup_id = create_duplicate_id(local_doc)
        local_docs[dup_id] = local_doc

    # Read archive from file
    print('Appending to local archive...')
    file_name = './backups/db_archive.json'
    if os.stat(file_name).st_size != 0:
        with open(file_name) as backup_file:
            backups = json.load(backup_file)
    else:
        backups = dict()

    # add to archive dictionary
    backups[str(datetime.datetime.now())] = local_docs

    # Write back to file
    with open(file_name, 'w') as backup_file:
        json.dump(backups, backup_file, indent=4)

    print('Collection successfully added to local archive')



def get_collection_as_dict(collection, raw=False, address=False, location=False):
    """
    Copy remote collection and return as dict.
    """
    # Copy documents to local dictionary (creating duplicate keys)
    local_docs = dict()
    documents = collection.find()
    for db_document in documents:
        local_doc = {key:db_document[key] for key in db_document}
        if address:
            dup_id = ''.join((local_doc['address'].upper()).split())
        elif location:
            dup_id = local_doc['address']
            if '_id' in local_doc:
                local_doc['_id'] = str(local_doc['_id'])
        else:
            dup_id = create_duplicate_id(local_doc, unit_id=raw)
            
        local_docs[dup_id] = local_doc

    return local_docs



def get_collection_as_list(collection):
    """
    Copy remote collection and return as list.
    """
    local_docs = []
    documents = collection.find()
    for document in documents:
        local_doc = {key:document[key] for key in document}
        local_docs.append(local_doc)

    return local_docs