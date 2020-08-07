# Description: Web Scraper for Appfolio Sites of Type 1
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring


from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
from pprint import pprint
from requests import get
import itertools
import argparse
import time
import json


class AppfolioV1:
    """
    Web Scraper for Appfolio Sites of Type 1.
    """

    def __init__(self, provider, city, state, college, url, alt_url_extension=None):
        """
        Initialize attributes.
        """
        self.provider = provider
        self.url = url
        self.city = city.lower()
        self.state = state.lower()
        self.college = college.lower()
        self.alt_url_extension = alt_url_extension


    def fetch_results(self):
        """
        Capture Inner-HTML of page at given URL, return as BS4 object.
        """
        # Simulate browser request with custom header
        headers = ({'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

        # Request html and build BS4 object
        url = self.url + '/' + self.city
        if self.alt_url_extension:
            url = self.url + '/' + self.alt_url_extension
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(3)
        page = driver.page_source
        driver.quit()
        html_soup = BeautifulSoup(page, 'html.parser')
        return html_soup.find_all('div', class_="listing-item")


    def parse_page(self, listing):
        """
        Scrape details from given listings, write to JSON.
        """
        try:
            # Grab sectional data tags (large hunks with details inside)
            full_address = listing.find('h2', class_='address')
            if full_address and self.city not in full_address.text.lower():
                skip_listing(self.college, 'city', self.provider)
                return

            # Grab detailed data tags
            address, unitNum = find_unit_num(full_address.text)
            price = listing.find('h3', class_="rent")
            if not price:
                skip_listing(self.college, 'data', self.provider)
                return
            else:
                price_low, price_high = find_prices(price.text)
            image = listing.find('div', class_="slider-image")
            image = image["data-background-image"]
            beds = listing.find('div', class_='feature beds')
            baths = listing.find('div', class_='feature baths')
            sqft = listing.find('div', class_='feature sqft')
            dogs = listing.find('div', class_='feature dogs')
            cats = listing.find('div', class_='feature cats')
            available = listing.find('div', class_="available")
            detail_link = listing.find('a', class_="apm-view-details btn secondary-btn")['href']

            # Parse text from tags
            beds = int(beds.text[0]) if (beds and ('studio' not in beds.text.lower())) else 1
            baths = float(baths.text.split()[0]) if baths else 1.0
            sqft = sqft.text.split()[0] if sqft else None
            if sqft:
                sqft = int(''.join([char for char in sqft if char.isdigit()]))
            pets = True if (dogs or cats) else None
            if available:
                available = available.find('strong').text
            if available == 'NOW':
                available = 'Now' 

            # Build document for DB
            unit = {
                "address": address,
                "unitNum": unitNum,
                "price_high": price_high,
                "price_low": price_low,
                "beds": beds,
                "baths": baths,
                "pets": pets,
                "sqft": sqft,
                "provider": self.provider,
                "images": [image],
                "URL": self.url + detail_link,
                'original_site': None,
                "available": available
            }
            # Send to JSON
            write_to_raw_json(unit, self.college)

        except Exception as e:
            skip_listing(self.college, 'error', self.provider)
            write_to_error_log(self.college, self.provider, e, link=self.url)


    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f'AppfolioV1 - {self.provider}')
        results = self.fetch_results()
        if not results:
            print('Cannot fetch results')
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
with open(f'{college}/appfolioV1Sites.json', 'r') as site_file:
    sites = json.load(site_file)

for site in sites[city]:
    a = AppfolioV1(site['provider'], city, state, college, site['URL'], site['extension'])
    a.get_data()
    