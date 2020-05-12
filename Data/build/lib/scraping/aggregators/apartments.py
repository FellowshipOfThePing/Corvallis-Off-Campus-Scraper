# Description: Web Scraper for Apartments.com
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Make it so if the listing has no price range (only one single range) it doesn't activate the subscraper 
# (Maybe reconsider this... We will need amenities later)
# TODO: Paginate scraper

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

class Apartments:
    """
    Scraper object for Apartments.com
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


    def find_detail_links(self):
        """
        Scrape search results page for links to detail pages. Return links.
        """
        url = f"https://www.apartments.com/apartments/{self.city}-{self.state}/"
        response = self.get_response(url)
        if response:
            html_soup = BeautifulSoup(response.text, 'html.parser')
            listing_container = html_soup.find('div', class_='placardContainer')
            listings = listing_container.find_all('article', class_='placard')
            listings = [listing.find('a', class_='placardTitle') for listing in listings]
            links = [listing['href'] for listing in listings if listing]
            return links
        else:
            return None


    def scrape_details(self, links):
        """
        Scrape details pages from given links. Write results to JSON file.
        """
        for link in links:
            try:
                response = self.get_response(link)
                html_soup = BeautifulSoup(response.text, 'html.parser')

                # Get details that are universal to each listing (building address, image, pet policy)
                street_address = html_soup.find('div', class_='propertyAddress').find('span').text
                if not (''.join([x for x in street_address if x.isdigit()])):
                    street_address = html_soup.find('div', class_='crumbs').find_all('span')[-1].text
                imageBox = html_soup.find(class_='carouselContent js-carouselContent')
                images = imageBox.find_all('div', class_='itemInner')
                images = [imtag.find('img')['src'] for imtag in images]
                pets = html_soup.find('div', class_='petPolicyDetails')
                if pets:
                    pets = False if 'No Pets Allowed' in pets.text else True
                else:
                    pets = None

                # Get floor plan option tags
                rentals = html_soup.find('div', class_='tabContent active')
                if rentals:
                    rentals = rentals.find_all('tr', class_='rentalGridRow')  
                else: 
                    rentals = html_soup.find_all('tr', class_='rentalGridRow')

                # Get details for corresponding floor plans
                for rental in rentals:
                    try:
                        # Price Range
                        price = rental.find('td', class_='rent')                            
                        if not price:
                            skip_listing(self.college, 'data', 'Apartments')
                            continue
                        else:
                            price = price.text
                        price_low = None
                        if '-' in price:
                            price_list = price.split('-')
                            price_low = ''.join([x for x in price_list[0] if x.isdigit()])
                            price_low = int(price_low) if price_low else None
                        price_high = ''.join([x for x in price[price.find('-'):] if x.isdigit()])
                        if not price_high:
                            skip_listing(self.college, 'data', 'Apartments')
                            continue
                        price_high = int(price_high)
                        
                        unitNum = rental.find('td', class_='unit').text
                        unitNum = '#' + unitNum.strip() if unitNum else None
                        beds = rental.find('td', class_='beds').text
                        beds = 1 if 'studio' in beds.lower() else int(beds.strip()[0])      # Beds
                        baths = float((rental.find('td', class_='baths').text).strip()[0])  # Baths
                        sqft = rental.find('td', class_='sqft').text                        # Sqft
                        if sqft:
                            sqft = int(sqft.split()[0].replace(',', ''))                    
                        available = (rental.find('td', class_='available').text).strip()    # Available
                        if 'now' in available.lower():
                            available = 'Now'
                        elif not (''.join([x for x in available if x.isdigit()])):
                            skip_listing(self.college, 'data', 'Apartments')
                            continue
                        original_site = html_soup.find('a', class_="js-externalUrl")
                        original_site = original_site['href'] if original_site else None

                        # Build document for DB
                        unit = {
                            'address': street_address,
                            'unitNum': unitNum,
                            'price_high': price_high,
                            'price_low': price_low,
                            'beds': beds,
                            'baths': baths,
                            'pets': pets,
                            'sqft': sqft,
                            'provider': 'Apartments',
                            'images': images,
                            'URL': link,
                            'original_site': original_site,
                            'available': available
                        }
                        write_to_raw_json(unit, self.college)

                    except AttributeError as e:
                        skip_listing(self.college, 'error', 'Apartments')
                        continue

            # Print Scraping errors and write to log file
            except Exception as e:
                write_to_error_log(self.college, 'Apartments', e, link=link)
                skip_listing(self.college, 'error', 'Apartments')
                continue

    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f"Apartments.com: {self.city}-{self.state}")
        links = self.find_detail_links()
        if links:
            listings = self.scrape_details(links)
        else:
            skip_scraper(self.college, 'Apartments')



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
a = Apartments(city, state, college)
a.get_data()
