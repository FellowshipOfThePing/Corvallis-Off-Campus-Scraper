# Description: Web Scraper for Zillow.com

# TODO: On top of JSON scraper, scrape image links directly from html as well
# TODO: Make it so that it only activates the subscraper if it NEEDS to. If the listing is alrady in the DB, skip.
# This could be done if, while scraping the listings pages, you collect tuples of links and duplicate_ids.
# This way, you can check if a duplicate ID is already in the database/backup before activating sub-scraper? Maybe?
# TODO: Multi-threading?
# TODO: Instead of waiting for proxies to be depleted, just get new proxy everytime one is blacklisted?
# TODO: Start timing GET requests to find average time between successes


from requests.exceptions import ProxyError, SSLError
from requests.exceptions import ContentDecodingError
from urllib.request import Request, urlopen, ProxyHandler
from pymongo import MongoClient
from bs4 import BeautifulSoup
from termcolor import colored
from scraping.proxies import *
from scraping.loggers import *
from scraping.utils import *
from itertools import cycle
from lxml import html
import traceback
import requests
import argparse
import signal
import random
import time
import json
import sys
import os


class Zillow:
    """
    Scraper object for Zillow.com
    Takes city, state, and college as string parameters
    """
    def __init__(self, city, state, college):
        self.city = city.lower()
        self.state = state.lower()
        self.college = college
        self.listing_dicts = []
        self.total_pages = 1
        clear_proxy_log()
        clear_proxy_stats()
        clear_proxy_blacklist()
        self.proxy_set = get_proxies()
        self.proxy_pool = cycle(self.proxy_set)


    def clean(self, text):
        """
        Clean given data
        """
        if text:
            return ' '.join(' '.join(text).split())
        return None


    def get_headers(self):
        """
        Retreive headers as params for GET request.
        """
        # Creating headers.
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, sdch, br',
            'accept-language': 'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4',
            'cache-control': 'max-age=0',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}
        return headers

    
    def get_proxied_response(self, url):
        """
        Get HTML from url_page
        """
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        ProxyHandler({'https': next(self.proxy_pool)})
        response = urlopen(req).read()
        return response


    def parse_json_from_html(self, response):
        """
        Find JSON from given html text.
        """
        parser = html.fromstring(str(response))
        raw_json_data = parser.xpath('//script[@data-zrr-shared-data-key="mobileSearchPageStore"]//text()')
        if not raw_json_data:
            skip_scraper(self.college, 'Zillow')
            return
        clean_json = self.clean(raw_json_data).replace('<!--', "").replace("-->", "")
        return clean_json


    def parse_listings_from_json(self, json_data):
        """
        Find and save listing data from given json
        """
        try:
            # Load JSON into Dict
            json_data = json.loads(json_data)

            # Write to file (for debugging only)
            # with open("test.json", 'w') as jf:
            #     json.dump(json_data, jf, indent=4)

            # Find if last page


            # Get search results from JSON
            if 'cat1' in json_data.keys():
                search_results = json_data['cat1']['searchResults']['listResults']
                if self.total_pages == 1:
                    self.total_pages = json_data['cat1']['searchList']['totalPages']
            else:
                search_results = json_data['searchResults']['listResults']
                if self.total_pages == 1:
                    self.total_pages = json_data['searchList']['totalPages']

            # Iterate through listings, saving to Raw listings JSON
            for prop in search_results:
                try:
                    address = prop['addressStreet']
                    address, unitNum = find_unit_num(address)
                    if "BED" in address or "BATH" in address.upper():
                        continue
                    image = prop['imgSrc']
                    baths = None
                    sqft = None
                    if 'baths' in prop:
                        baths = float(prop['baths'])
                    if 'area' in prop:
                        sqft = prop['area']
                        sqft = int(sqft) if sqft else None
                    if 'latLong' in prop:
                        latitude = prop['latLong']['latitude']
                        longitude = prop['latLong']['longitude']
                    else:
                        latitude = None
                        longitude = None
                    provider = 'Zillow'
                    property_url = prop['detailUrl']
                    available = 'Now'
                    # If single unit
                    if 'hdpData' in prop and 'unit' in prop['hdpData']['homeInfo']:
                        unitNum = prop['hdpData']['homeInfo']['unit']
                        unitNum = '#' + unitNum if unitNum != ' ' else None
                        price = prop['price']
                        price_low, price_high = find_prices(price)
                        beds = int(prop['beds'])
                        if beds == 0:
                            beds = 1
                        data = {
                            'address': address,
                            'unitNum': unitNum,
                            'price_high': price_high,
                            'price_low': price_low,
                            'beds': beds,
                            'baths': baths,
                            'pets': None,
                            'sqft': sqft,
                            'provider': provider,
                            'images': [image],
                            'URL': property_url,
                            'original_site': None,
                            'available': available,
                            'latitude': latitude,
                            'longitude': longitude
                        }
                        write_to_raw_json(data, self.college)
                    # else if multiple units
                    elif 'units' in prop:
                        for unit in prop['units']:
                            price = unit['price']
                            price_low, price_high = find_prices(price)
                            beds = int(unit['beds'])
                            if beds == 0:
                                beds = 1
                            data = {
                                'address': address,
                                'unitNum': unitNum,
                                'price_high': price_high,
                                'price_low': price_low,
                                'beds': beds,
                                'baths': baths,
                                'pets': None,
                                'sqft': sqft,
                                'provider': provider,
                                'images': [image],
                                'URL': property_url,
                                'original_site': None,
                                'available': available,
                                'latitude': latitude,
                                'longitude': longitude
                            }
                            write_to_raw_json(data, self.college)
                
                except AttributeError as e:
                    write_to_error_log(self.college, 'Zillow', e)
                    skip_listing(self.college, 'error', 'Zillow')
                    continue

        except Exception as e:
            write_to_error_log(self.college, 'Zillow', e)


    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f"Zillow.com: {self.city}-{self.state}")
        page_num = 1
        while page_num <= self.total_pages:
            print(f'Zillow Page {page_num}')
            page_url = f"https://www.zillow.com/{self.city}-{self.state}/rentals/{page_num}_p/"
            response = self.get_proxied_response(page_url)
            json_data = self.parse_json_from_html(response)
            if json_data:
                self.parse_listings_from_json(json_data)
                page_num += 1
            else:
                return



# Construct CLI Arguments and parse input
ap = argparse.ArgumentParser()
ap.add_argument("-u", "--university", required=True, help="College to Scrape listings for")
ap.add_argument("-c", "--city", required=True, help="College to Scrape listings for")
ap.add_argument("-s", "--state", required=True, help="State to Scrape listings for")
args = vars(ap.parse_args())

college = args['university']
city = args['city']
state = args['state']

# Run Scraper
z = Zillow(city, state, college)
z.get_data()
