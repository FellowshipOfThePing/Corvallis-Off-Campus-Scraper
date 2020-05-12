# Description: Web Scraper for JPM Real Estate
# Last Update: 04/08/20
# Update Desc: Connecting to listing file and error log + refactoring

# TODO: Refactor/Test after inserting capture_page

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from scraping.loggers import *
from scraping.utils import *

# Print provider name to console
provider = "JPM Real Estate"
print(provider)

is_next_page = True
next_page = 1
starting_listing = 0

while is_next_page:
    # Request html and build BS4 object
    url = f"https://jpm-re.com/rental-units/?rmwebsvc_command=Search_Result.aspx&rmwebsvc_template=unit&rmwebsvc_Page={next_page}&rmwebsvc_start={starting_listing}&rmwebsvc_corpid=jpm&rmwebsvc_mode=JavaScript&rmwebsvc_locations=1&maxperpage=15&citylk=&bedroomslk=&bathroomslk=&marketrentle=&unituserdef_Allow_on_websitelk=yes&propuserdef_web_displaylk=yes"
    html_soup = capture_page(url)

    # Find listings in HTML
    house_containers = html_soup.find_all('div', class_="unit-list")

    if not house_containers:
        skip_scraper('OSU', provider)

    # Iterate through listings and upload information to local JSON file
    for listing in house_containers:
        try:
            # Grab individual pieces of info
            address = listing.find('div', class_='unit-address').text
            if 'corvallis' not in address.lower():
                skip_listing('OSU', 'city', provider)
                continue
            address = address[:address.lower().find('corvallis')]
            address, unitNum = find_unit_num(address)
            features = (listing.find('div', class_='unit-stats').text).split('|')
            price = features[2].split()[0]
            if price:
                price = int(float(price))
            else:
                skip_listing('OSU', 'data', provider)
                continue
            image = listing.find('img')['src']
            bedbath = features[3].split()
            beds = int(bedbath[1])
            baths = float(bedbath[3])
            sqft = None
            available = (listing.find('div', class_='unit-stats').find_all('b')[2].text).split('}')[-1]
            link = "https://jpm-re.com" + listing.find('a')['href'],
            unit = {
                "address": address,
                "unitNum": unitNum,
                "price_high": price,
                "price_low": None,
                "beds": beds,
                "baths": baths,
                "pets": None,
                "sqft": sqft,
                "provider": provider,
                "images": [image],
                "URL": link,
                "available": available,
                "original_site": None
            }
            # Send to DB
            write_to_raw_json(unit, 'OSU')

        except Exception as e:
            skip_listing('OSU', 'error', provider)
            write_to_error_log('OSU', provider, e, link=url)
                
    if 'Next' in html_soup.find('div', class_='NavigationPage').text:
        next_page += 1
        starting_listing = ((next_page - 1) * 15) + 15
    else:
        is_next_page = False