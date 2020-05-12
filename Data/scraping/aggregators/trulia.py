# Description: Web Scraper for Trulia.com
# Last Update: 04/08/20
# Update Desc:

# TODO: Make it so if the listing has no price range (only one single range) it doesn't activate the subscraper
# TODO: Paginate scraper
# TODO: NOT DONE - Work out bugs before adding to Aggregate scrapers folder

from urllib.request import Request, urlopen
from pymongo import MongoClient
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
from lxml import html
import traceback
import requests
import argparse
import json
import sys
import os

class Trulia:
    """
    Scraper object for Trulia.com
    Takes city & state as string parameters
    """
    def __init__(self, city, state, college):
        self.city = city.lower()
        self.state = state.lower()
        self.college = college


    def get_headers(self):
        """
        Retreive headers as params for GET request.
        """
        # Creating headers.
        headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'accept-encoding': 'gzip, deflate, sdch, br',
                'accept-language': 'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4',
                'cache-control': 'max-age=0',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}
        return headers


    def get_response(self, url):
        """
        Get response from given URL, save to file and return response.
        """
        # Getting response from zillow.com.
        for i in range(5):
            response = requests.get(url, headers=self.get_headers())
            if response.status_code != 200:
                continue
            else:
                return response
        return None


    def find_details_json(self, url):
        """
        Scrape search results page for links to detail pages. Return links.
        """
        response = self.get_response(url)
        if response:
            html_soup = BeautifulSoup(response.text, 'html.parser')
            listings_json = html_soup.find('script', id='__NEXT_DATA__')
            if listings_json:
                listings_json = str(listings_json)
                listings_json = listings_json.replace("<script id=\"__NEXT_DATA__\" type=\"application/json\">", "").replace("</script>", "")
                listings = json.loads(listings_json)
                return listings
            else:
                skip_scraper(self.college, 'Trulia')


    def scrape_details(self, listings_dict, url):
        """
        Scrape details pages from given links. Write results to JSON file.
        """
        try:
            next_page_url = None
            if "paginationNext" in listings_dict['props']['_page']['linkTags']:
                next_page_url = listings_dict['props']['_page']['linkTags']['paginationNext']['href']
            listings = listings_dict['props']['searchData']['homes']
            for listing in listings:
                try:
                    full_address = listing['location']['partialLocation']
                    address, unitNum = find_unit_num(full_address)
                    if address == "Address Not Disclosed":
                        skip_listing(self.college, 'data', 'Trulia')
                        continue
                    full_price = listing['price']['formattedPrice']
                    price_low, price_high = find_prices(full_price)
                    beds = listing['bedrooms']['formattedValue']
                    beds = ''.join([x for x in beds if x.isdigit()])
                    if beds:
                        if '-' in beds:
                            beds = int(beds[:beds.find('-')])
                        else:
                            beds = int(beds)
                    else:
                        beds = None
                    baths = listing['bathrooms']['formattedValue']
                    baths = ''.join([x for x in baths if not x.isalpha()])
                    if baths:
                        if '-' in baths:
                            baths = float(baths[:baths.find('-')])
                        else:
                            baths = float(baths)
                    else:
                        baths = None
                    sqft = None
                    if 'floorSpace' in listing and listing['floorSpace']:
                        sqft = listing['floorSpace']['formattedDimension']
                        sqft = int(''.join([x for x in sqft if x.isdigit()])) if sqft else None
                    tags = listing['tags']
                    pets = None
                    for tag in tags:
                        if "PET FRIENDLY" in tag.values():
                            pets = True
                    photos = listing['media']['photos']
                    images = set()
                    for photo in photos:
                        images.add(photo['url']['small'])
                    detail_link = 'https://www.trulia.com' + listing['url']
                    latitude = listing['location']['coordinates']['latitude']
                    longitude = listing['location']['coordinates']['longitude']
                    # Build document for DB
                    unit = {
                        'address': address,
                        'unitNum': unitNum,
                        'price_high': price_high,
                        'price_low': price_low,
                        'beds': beds,
                        'baths': baths,
                        'pets': pets,
                        'sqft': sqft,
                        'provider': 'Trulia',
                        'images': list(images),
                        'URL': detail_link,
                        'original_site': None,
                        'available': 'Now',
                        'latitude': latitude,
                        'longitude': longitude
                    }
                    write_to_raw_json(unit, self.college)

                # Print Scraping errors and write to log file
                except Exception as e:
                    write_to_error_log(self.college, 'Trulia', e, link=url)
                    skip_listing(self.college, 'error', 'Trulia')
                    continue

        except Exception as e:
            write_to_error_log(self.college, 'Trulia', e, link=url)
            skip_listing(self.college, 'error', 'Trulia')

        return next_page_url


    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f"Trulia.com: {self.city}-{self.state}")
        page_url = f"https://www.trulia.com/for_rent/{self.city},{self.state}"
        page_count = 1
        while page_url:
            print(f'Page {page_count} Results')
            details_dict = self.find_details_json(page_url)
            page_url = self.scrape_details(details_dict, page_url)
            page_count += 1



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
t = Trulia(city, state, college)
t.get_data()
