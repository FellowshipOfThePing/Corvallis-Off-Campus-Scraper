# Description: Web Scraper for Trinity Property Management
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring


from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *
from selenium import webdriver
from bs4 import BeautifulSoup
import time

# Connect to MongoDB
provider = "BeaverPads"
print(provider)

# Request html and build BS4 object
url = "http://beaverpads.com/properties/"
html_soup = capture_page(url)

house_containers = html_soup.find_all('article', class_="clearfix property-item-box")

if not house_containers:
    skip_scraper('OSU', provider)

# Iterate through listings and upload information to local JSON file
for listing in house_containers:
    try:
        sold_out = listing.find('img')['src']
        if 'SOLD-OUT' in sold_out:
            skip_listing('OSU', 'unavailable', provider)
            continue
        address = listing.find('h3').text
        address = address[:address.find(':')]
        detail_link = listing.find('a', class_='overlay')['href']
        detail_page = capture_page(detail_link)
        info = detail_page.find('div', class_='info')
        price = info.find('span', class_='price').text
        price = int(''.join([x for x in price if x.isdigit()]))
        features = info.find('div', class_='table').find_all('span', class_='item value')
        beds = int(features[0].text)
        baths = float(features[1].text)
        sqft = int(features[3].text)
        available = features[4].text
        slides = detail_page.find('ul', class_='slides').find_all('img')
        images = [img['src'] for img in slides]
        for i in range(len(images)):
            images[i] = images[i].replace('-138x140', '')
        unit = {
            "address": address,
            "unitNum": None,
            "price_high": price,
            "price_low": None,
            "beds": beds,
            "baths": baths,
            "pets": None,
            "sqft": sqft,
            "provider": provider,
            "images": images,
            "URL": detail_link,
            "available": available,
            "original_site": None,
            
        }
        # Send to JSON
        write_to_raw_json(unit, 'OSU')

    except Exception as e:
        skip_listing('OSU', 'error', provider)
        write_to_error_log('OSU', provider, e, link=detail_link)
