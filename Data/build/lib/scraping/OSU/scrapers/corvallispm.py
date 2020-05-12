# Description: Web Scraper for Corvallis Property Management LLC
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *

provider = "Corvallis Property Management LLC"
print(provider)

# Request html and build BS4 object
url = "https://corvallispm.managebuilding.com/Resident/public/rentals"
html_soup = capture_page(url)

# Get detail links
detail_links = html_soup.find_all('a', class_='featured-listing')

if not detail_links:
    skip_scraper('OSU', provider)

# Parse listings
for link in detail_links:
    try:
        address = (link.find_all('h3', class_="featured-listing__title")[0].text).strip('- .')
        address, unitNum = find_unit_num(address)
        price_high = link['data-rent']
        beds = int(link['data-bedrooms'])
        baths = float(link['data-bathrooms'])
        sqft = int(link['data-square-feet'])
        if sqft == 0:
            sqft = None
        detail_url_ending = link['href']
        detail_url = f"https://corvallispm.managebuilding.com{detail_url_ending}"
        detail_page = capture_page(detail_url)
        image_gallery = detail_page.find('div', class_='unit-detail__gallery')
        images = [img['src'] for img in image_gallery.find_all('img')]
        available = detail_page.find('div', class_='unit-detail__available-date')
        if available:
            available = (''.join([x for x in available.text if not x.isalpha()])).strip()
        else:
            available = None
        unit = {
            "address": address,
            "unitNum": unitNum,
            "price_high": price_high,
            "price_low": None,
            "images": images,
            "beds": beds,
            "baths": baths,
            "pets": None,
            "sqft": sqft,
            "provider": provider,
            "URL": detail_url,
            "available": available,
            "original_site": None
        }
        # Send document to JSON
        write_to_raw_json(unit, 'OSU')
    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)