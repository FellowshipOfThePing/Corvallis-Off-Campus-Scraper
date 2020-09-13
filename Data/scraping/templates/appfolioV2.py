# Description: Web Scraper for Appfolio Sites of Type 2
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Most photos are not rendering before capture, leaving image urls = null. See if we can change that. 
# TODO: REVISE TO SCRAPE JSON FROM HTML

from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
from pprint import pprint
from requests import get
import itertools
import traceback
import argparse
import time
import json
import sys


class AppfolioV2:
    """
    Web Scraper for Appfolio Sites of Type 2.
    """

    def __init__(self, provider, url_prefix, city, state, college):
        """
        Initialize attributes.
        """
        self.provider = provider
        self.url_prefix = url_prefix
        self.city = city.lower()
        self.state = state.lower()
        self.college = college.lower()


    def fetch_results(self):
        """
        Capture Inner-HTML of page at given URL, return as BS4 object.
        """
        # Simulate browser request with custom header
        headers = ({'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

        # Request html and build BS4 object
        url = f"https://{self.url_prefix}.appfolio.com/listings"
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(3)
        page = driver.page_source
        driver.quit()
        html_soup = BeautifulSoup(page, 'html.parser')
        return html_soup.find_all('div', class_="listing-item result js-listing-item")


    def parse_page(self, listing):
        """
        Scrape details from given listings, write to JSON.
        """
        try:
            # Find address (if none, skip listing) + image, pet policy, & detail link
            address = listing.find('span', class_='u-pad-rm js-listing-address')
            if not address or ('corvallis' not in address.text.lower()):
                skip_listing(self.college, 'city', self.provider)
                return
            address, unitNum = find_unit_num(address.text)
            image = listing.find('img', class_='listing-item__image lazy')
            image = image['src'] if image else None
            pets = False if listing.find('span', class_='js-listing-pet-policy') else None
            detail_link = f"https://{self.url_prefix}.appfolio.com" + listing.find('a', class_="btn btn-secondary js-link-to-detail")['href']

            # Set default feature values
            price = None
            beds = 1
            baths = 1.0
            sqft = None
            available = 'Unknown'

            # Scrape feature values
            features = listing.find_all('dl')
            for feature in features:
                label = feature.find('dt').text.lower()
                value = feature.find('dd').text
                if 'rent' in label:
                    price = int((value[1:]).replace(',', ''))
                if 'bed' in label and value.split()[0].isdigit():
                    beds = int(value.split()[0])
                if 'bath' in label:
                    baths = value.split()
                    if len(baths) >= 3:
                        baths = float(baths[3])
                    else:
                        baths = 1.0
                if 'sq' in label:
                    sqft = int((value).replace(',', ''))
                if 'available' in label:
                    available = value.title()
            if not price:
                skip_listing(self.college, 'data', self.provider)
                return

            # Build document for DB
            unit = {
                "address": address,
                "unitNum": unitNum,
                "price_high": price,
                "price_low": None,
                "beds": beds,
                "baths": baths,
                "pets": pets,
                "sqft": sqft,
                "provider": self.provider,
                "images": [image],
                "URL": detail_link,
                'original_site': None,
                "available": available
            }
            # Send to JSON
            write_to_raw_json(unit, self.college)

        except Exception as e:
            skip_listing(self.college, 'error', self.provider)
            write_to_error_log(self.college, self.provider, e, link=f"https://{self.url_prefix}.appfolio.com/listings")


    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f'AppfolioV2 - {self.provider}')
        results = self.fetch_results()
        if not results:
            print('Cannot fetch results')
            skip_scraper(self.college, self.provider)
            return
        for result in results:
            self.parse_page(result)
            

# Construct CLI Arguments and parse input
ap = argparse.ArgumentParser()
ap.add_argument("-u", "--university", required=True, help="College to Scrape listings for")
ap.add_argument("-c", "--city", required=True, help="College to Scrape listings for")
ap.add_argument("-s", "--state", required=True, help="State to Scrape listings for")
args = vars(ap.parse_args())

college = args['university']
city = args['city']
state = args['state']

# Call Scraper for all sites listed in College's appfolioV1Sites.json file
with open(f'{college}/appfolioV2Sites.json', 'r') as site_file:
    sites = json.load(site_file)

for site in sites[city]:
    a = AppfolioV2(site['provider'], site['URL_prefix'], city, state, college)
    a.get_data()