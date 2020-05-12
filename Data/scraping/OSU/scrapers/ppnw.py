# Description: Web Scraper for Preferred Properties NW
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *


# Print provider name to console
provider = "Preferred Properties NW"
print(provider)

# Request html and build BS4 object
url = "https://www.ppnw.com/locations"
html_soup = capture_page(url)

# Find listings in HTML
house_containers = html_soup.find_all('div', class_="views-row-unformatted")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        link = "https://www.ppnw.com" + listing.find('a')['href']
        available = (listing.find('div', class_='clearfix').text).split()[1:]
        available = ' '.join([x.title() if type(x) == str else x for x in available])
        detail_page = capture_page(link)
        address = detail_page.find('h1', id='page-title').text
        address, unitNum = find_unit_num(address)
        address = address.strip()
        price = detail_page.find('div', class_='field-detailed-price-value').find('p').text
        price = price.split()[0]
        price = int(float(''.join([x for x in price if (x.isdigit() or x == '.')])))
        beds = detail_page.find('div', class_='field-bedrooms-value').text
        beds = int(''.join([x for x in beds if x.isdigit()]))
        sqft = detail_page.find('div', class_='field-square-footage-value').text
        sqft = int(''.join([x for x in sqft if x.isdigit()]))
        pets = detail_page.find('div', class_='field-pets-value')
        imageBox = detail_page.find('div', id='content-aside').find_all('img')
        images = ["https://www.ppnw.com/sites/default/files/imagecache/property_first/property-images/" + img['src'].split('/')[-1] for img in imageBox]
        if pets and 'not' in pets.text:
            pets = False
        else:
            pets = True
        unit = {
            "address": address,
            "unitNum": unitNum,
            "price_high": price,
            "price_low": None,
            "beds": beds,
            "baths": None,
            "pets": pets,
            "sqft": sqft,
            "provider": provider,
            "images": images,
            "URL": link,
            "available": available,
            "original_site": None
        }
        # Send to JSON
        write_to_raw_json(unit, 'OSU')

    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=url)