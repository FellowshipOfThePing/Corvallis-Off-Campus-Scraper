# Description: Web Scraper for Witham Oaks Living Community
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
import time
import json
import sys

# Print provider name to console
provider = "Witham Hill Oaks"
print(provider)

# Request html and build BS4 object
url = "https://www.withamhilloaks.com/floorplans.aspx"
html_soup = capture_page(url)

# Find listings in HTML
house_containers = html_soup.find_all('div', class_="tab-pane")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        info = listing.find_all('tr')
        beds = int(info[0].find_all('td')[1].text)
        baths = float(info[1].find_all('td')[1].text)
        sqft = info[2].find_all('td')[1].text
        sqft = int(sqft.replace(',', ''))
        image = listing.find_all('img', class_='fp_thumb lazy')[0]['data-src']
        floor_plan = listing.find('button', class_='applyButton')
        if floor_plan:
            floor_plan = floor_plan['onclick'].split('=')[-1]
            floor_plan = int(''.join([x for x in floor_plan if x.isdigit()]))
        else:
            skip_listing('OSU', 'unavailable', provider)
            continue
        detail_link = f"https://www.withamhilloaks.com/availableunits.aspx?myOlePropertyId=868954&floorPlans={floor_plan}"
        detail_page = capture_page(detail_link)
        units = detail_page.find('tbody').find_all('tr', class_='AvailUnitRow')
        for unit in units:
            price_high = None
            price_low = None
            unitNum = None
            available = None
            for cell in unit.find_all('td'):
                if cell['data-label'] == 'Apartment':
                    unitNum = '#' + (cell.text[1:]).strip()
                elif cell['data-label'] == 'Rent':
                    price = cell.text
                    if '-' in price:
                        price = price.split('-')
                        price_low = int(''.join([x for x in price[0] if x.isdigit()]))
                        price_high = int(''.join([x for x in price[1] if x.isdigit()]))
                elif cell['data-label'] == 'Date Available':
                    available = cell.text
            if not price_high:
                skip_listing('OSU', 'data', provider)
                continue
        
            # Build documents
            unit = {
                "address": "4275 NW Clubhouse Pl",
                "unitNum": unitNum,
                "price_high": price_high,
                "price_low": price_low,
                "beds": beds,
                "baths": baths,
                "pets": True,
                "sqft": sqft,
                "provider": provider,
                "images": [image],
                "URL": detail_link,
                "available": available,
                "original_site": None
            }
            # Send to JSON
            write_to_raw_json(unit, 'OSU')
        
    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)