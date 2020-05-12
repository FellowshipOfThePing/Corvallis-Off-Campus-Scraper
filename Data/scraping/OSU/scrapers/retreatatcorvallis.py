# Description: Web Scraper for Retreat Corvallis
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Update this - No longer works with new page format
# TODO: Record in scrape stats log when a page is no longer getting ANY results

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *

provider = "Retreat at Corvallis"
print(provider)

# Request html and build BS4 object
url = "http://www.retreatcorvallis.com/Floor-Plans"
html_soup = capture_page(url)

# Find listings in HTML
house_containers = html_soup.find_all('div', class_="portfolio-item")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        # Grab pieces of info
        title = listing.find_all('h4', class_='title')[0].text
        address = "700 SW Chickadee St"
        unitNum = title[title.find('-')+1:].strip()
        image = 'http://www.retreatcorvallis.com' + listing.find('img')['src']
        info = listing.find_all('div', class_="fp-info has-description")[0]
        features = info.find_all('p')[1].text
        available = (info.find_all('div', class_='row')[0].find_all('p')[-1].text).strip()
        beds = int(title.split()[0])
        baths = float(title.split()[2])
        price = int((features[features.find('$'):features.find('$')+5]).strip()[1:]) * beds
        sqft = int((features[:features.find('|')]).strip().split()[0])
        if 'available' in available.lower():
            available = "Now"
        link = "http://www.retreatcorvallis.com" + info.find_all('a')[0]['href']
        # Build document for DB
        unit = {
            "address": address,
            "unitNum": unitNum,
            "price_high": price,
            "price_low": None,
            "beds": beds,
            "baths": baths,
            "pets": "Unknown",
            "sqft": sqft,
            "provider": provider,
            "images": [image],
            "URL": link,
            "available": available,
            "original_site": None
        }
        # Send to JSON
        write_to_raw_json(unit, 'OSU')

    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)
