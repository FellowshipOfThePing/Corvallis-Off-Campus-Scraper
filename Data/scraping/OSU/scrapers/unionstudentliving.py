# Description: Web Scraper for Union Student Living
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Revise this scraper in general

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
import time

provider = "The Union Student Living"
print(provider)

# Request html and build BS4 object
url = "https://www.livetheunion.com/corvallis/the-union/"
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
        price = (listing.find('div', class_='fp-col rent').text).split()[-1][1:]
        price_high = int(''.join([x for x in price if x.isdigit()]))
        bedbath = (listing.find('span', 'fp-col-text').text).split()
        beds = int(bedbath[0][0])
        baths = float(bedbath[-1][0])
        price_high *= beds
        detail_link = listing.find('a', class_='secondary-action')['href']
        detail_page = capture_page(detail_link)
        image = detail_page.find_all('div', class_='galleria-image')[-1].find('img')['src']
        sqft = detail_page.find('li', class_='fp-stats-item sq-feet').text
        sqft = int(''.join([x for x in sqft if x.isdigit()]))
        available = 'Unknown'
        unit = {
            "address": "2750 NW Harrison Blvd",
            "unitNum": None,
            "price_high": price_high,
            "price_low": None,
            "beds": beds,
            "baths": baths,
            "pets": False,
            "sqft": sqft,
            "provider": provider,
            "images": [image],
            "URL": detail_link,
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
