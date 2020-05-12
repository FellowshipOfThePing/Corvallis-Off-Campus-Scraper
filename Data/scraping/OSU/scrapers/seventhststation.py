# Description: Web Scraper for 7th Street Station Housing
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Refactor find_all()[0] into find() (probably applies to other scrapers as well)

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *

# Provider Title
provider = "7th Street Station"
print(provider)

# Request html and build BS4 object
url = "https://www.americancampus.com/student-apartments/or/corvallis/7th-street-station/floor-plans#/"
html_soup = capture_page(url)

# Find listings in HTML
house_containers = html_soup.find_all('div', class_="property")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        # Grab pieces of info
        title = listing.find('h2', class_='property-title').text
        available = listing.find_all('h6', class_="property-avail")[0].text
        images = listing.find_all('div', class_="property-image")[0].find('img')['src']
        beds = int(listing.find_all('h2', class_="property-title")[0].text[0])
        baths = float(listing.find_all('h2', class_="property-title")[0].text[8])
        price = listing.find_all('h6', class_="property-price")[0].text
        if '-' in price:
            price = price.split('-')
            price_low = int(''.join([x for x in price[0] if x.isdigit()]))
            price_high = int(''.join([x for x in price[1] if x.isdigit()]))
        else:
            price_high = int(''.join([x for x in price if x.isdigit()]))
            price_low = None
        sqft = int((listing.find_all('h6', class_="property-size")[0].text).split()[-1])
        if available != 'Available':
            skip_listing('OSU', 'unavailable', provider)
            continue
        # Build document for DB
        unit = {
            "address": "701 SW 7th Street",
            "unitNum": None,
            "price_high": price_high,
            "price_low": price_low,
            "beds": beds,
            "baths": baths,
            "pets": True,
            "sqft": sqft,
            "provider": provider,
            "images": [images],
            "URL": url,
            "available": 'Now',
            "original_site": None
        }
        # Send to JSON
        write_to_raw_json(unit, 'OSU')

    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)
