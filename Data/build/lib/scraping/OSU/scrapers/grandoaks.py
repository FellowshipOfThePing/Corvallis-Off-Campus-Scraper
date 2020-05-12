# Description: Web Scraper for Grand Oaks Community
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
import time
import json

# Print provider name to console
provider = "Grand Oaks Community"
print(provider)

# Request html and build BS4 object
url = "https://www.grandoakscommunity.com/corvallis-corvallis/grand-oaks-grand-oaks/"
html_soup = capture_page(url)

# Get location data from JSON
json_data = str(html_soup.find('script', type="application/ld+json"))
json_data = json_data.replace('<script type="application/ld+json">', '').replace('</script>', '')
json_dict = json.loads(json_data)
latitude = json_dict['geo']['latitude']
longitude = json_dict['geo']['longitude']

# Find listings in HTML
house_containers = html_soup.find_all('li', class_="fp-group-item")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        # Grab individual pieces of info
        price = (listing.find_all('div', class_='fp-col rent')[0].text).split()[-1][1:]
        price_high = int(price.replace(',', ''))
        image = listing.find_all('img')[0]['src']
        bedbath = (listing.find_all('span', 'fp-col-text')[0].text).split()
        beds = int(bedbath[0][0])
        baths = float(bedbath[-1][0])
        sqft = listing.find_all('div', class_='fp-col sq-feet')[0].find_all('span')[1].text
        sqft = int(sqft.replace(',', ''))
        available = listing.find_all('a', class_='primary-action js-waitlist-confirm')[0].text
        link = listing.find_all('a', class_="secondary-action")[0]['href']
        unit = {
            "address": "6300 SW Grand Oaks Dr",
            "unitNum": None,
            "price_high": price_high,
            "price_low": None,
            "beds": beds,
            "baths": baths,
            "pets": True,
            "sqft": sqft,
            "provider": provider,
            "images": [image],
            "URL": link,
            "available": available,
            "original_site": None,
            "latitude": latitude,
            "longitude": longitude
        }
        # Send to JSON
        write_to_raw_json(unit, 'OSU')

    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)