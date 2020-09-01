# Description: Web Scraper for Realtor.com

# TODO: On top of JSON scraper, scrape image links directly from html as well
# TODO: Make it so that it only activates the subscraper if it NEEDS to. If the listing is alrady in the DB, skip.
# This could be done if, while scraping the listings pages, you collect tuples of links and duplicate_ids.
# This way, you can check if a duplicate ID is already in the database/backup before activating sub-scraper? Maybe?
# TODO: Multi-threading?
# TODO: Instead of waiting for proxies to be depleted, just get new proxy everytime one is blacklisted?
# TODO: Start timing GET requests to find average time between successes


from requests.exceptions import ProxyError, SSLError
from requests.exceptions import ContentDecodingError
from urllib.request import Request, urlopen
from pymongo import MongoClient
from bs4 import BeautifulSoup
from termcolor import colored
from scraping.proxies import *
from scraping.loggers import *
from scraping.utils import *
from itertools import cycle
from lxml import html
import traceback
import requests
import argparse
import signal
import random
import time
import json
import sys
import os


class Realtor:
    """
    Scraper object for Realtor.com
    Takes city, state, and college as string parameters
    """

    def __init__(self, city, state, college):
        self.city = city.lower()
        self.state = state.lower()
        self.college = college
        self.listing_links = []
        clear_proxy_log()
        clear_proxy_stats()
        clear_proxy_blacklist()
        self.proxy_set = get_proxies()
        self.proxy_pool = cycle(self.proxy_set)

    def get_headers(self):
        """
        Retreive headers as params for GET request.
        """
        # Creating headers.
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
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
        seconds = random.randint(1, 10)
        print(f'{seconds} Second(s) Randomized Delay')
        time.sleep(seconds)
        for i in range(5):
            response = requests.get(url, headers=self.get_headers())
            if response.status_code != 200:
                continue
            else:
                return response
        return

    def handler(self, signum, frame):
        """
        Timeout handler for proxy requests.
        """
        raise TimeoutError

    def increment_failures(self, proxy):
        """
        Increment failure count of given proxy before writing to proxy_failures log, or blacklist.
        """
        if proxy[1] >= 2:
            self.blacklist_proxy(proxy)
            write_to_proxy_log(proxy, 'Realtor', blacklist=True)
        else:
            self.proxy_set.remove(proxy)
            temp = (proxy[0], proxy[1] + 1)
            self.proxy_set.add(temp)
            write_to_proxy_log(temp, 'Realtor')
            self.proxy_pool = cycle(self.proxy_set)

    def decrement_failures(self, proxy):
        """
        Decrement failure count of given proxy before writing to proxy_log.
        """
        if proxy[1] > 0:
            self.proxy_set.remove(proxy)
            temp = (proxy[0], proxy[1] - 1)
            self.proxy_set.add(temp)
            write_to_proxy_log(temp, 'Realtor', decrement=True)
            self.proxy_pool = cycle(self.proxy_set)

    def blacklist_proxy(self, proxy):
        """
        Remove given proxy from self.proxies, re-cycle proxy_pool.
        """
        print_red(f"BLACKLIST: {proxy[0]}")
        write_to_proxy_blacklist(proxy)
        write_to_proxy_log(proxy, 'Realtor', blacklist=True)
        self.proxy_set.remove(proxy)
        self.proxy_pool = cycle(self.proxy_set)

    def try_new_proxy(self, url, proxy):
        """
        GET request with new proxy IP.
        """
        try:
            start_time = time.time()
            response = requests.get(url, headers=self.get_headers(), proxies={
                                    "http": f'http://{proxy[0]}', "https": f'https://{proxy[0]}'})
            if response.status_code != 200:
                # No error but also no successful response
                return -1
            else:
                self.decrement_failures(proxy)
                adjust_proxy_mean_GET_success(start_time)
                return response

        except ContentDecodingError as e:
            print_yellow("Content Encoding Error.")
            increment_proxy_error("ContentEncoding")
            self.blacklist_proxy(proxy)
            adjust_proxy_mean_GET_failure(start_time)
            return

        except SSLError as e:
            print_yellow("SSL Error.")
            increment_proxy_error("SSL")
            self.blacklist_proxy(proxy)
            adjust_proxy_mean_GET_failure(start_time)
            return

        except ProxyError as e:
            print_yellow("Max retries exceeded with Proxy.")
            increment_proxy_error("Proxy")
            self.increment_failures(proxy)
            adjust_proxy_mean_GET_failure(start_time)
            return

        except (IndexError, TimeoutError) as e:
            print_yellow("Proxy Timeout.")
            increment_proxy_error("Timeout")
            self.blacklist_proxy(proxy)
            adjust_proxy_mean_GET_failure(start_time)
            return

        except Exception as e:
            print_yellow("Error in try_new_proxy")
            increment_proxy_error("Other")
            write_to_error_log(self.college, "Realtor", e)
            adjust_proxy_mean_GET_failure(start_time)
            return

    def get_proxied_response(self, url):
        """
        GET response from given URL with proxy, save to file and return response.
        On Error, increment failures in Proxy log, or blacklist proxy, depending on error Type.
        """
        response = None
        while not response:
            if len(self.proxy_set) > 6:
                # Cycle proxies, setup timeout signal
                proxy = next(self.proxy_pool)
                signal.signal(signal.SIGALRM, self.handler)
                signal.alarm(10)

                # Attempt GET. Catch various errors.
                try:
                    print(f'\nGET with proxy: {proxy[0]}')
                    response = self.try_new_proxy(url, proxy)
                    if response == -1:
                        print_yellow('Non-200 Response')
                        self.increment_failures(proxy)
                        response = None

                except ProxyError as e:
                    print_yellow("Skipping. Connection error")
                    increment_proxy_error("Proxy")
                    self.increment_failures(proxy)
                    response = None

                except (IndexError, TimeoutError) as e:
                    print_yellow("Proxy Timeout.")
                    increment_proxy_error("Timeout")
                    self.blacklist_proxy(proxy)
                    response = None

                except Exception as e:
                    print_yellow("Error in get_proxied_response")
                    increment_proxy_error("Other")
                    write_to_error_log(self.college, "Realtor", e)
                    response = None

                finally:
                    signal.alarm(0)

            # If needed, update proxy pool
            else:
                print_blue('\nUPDATING PROXIES')
                self.proxy_set = get_proxies(self.proxy_set)
                self.proxy_pool = cycle(self.proxy_set)
                clear_proxy_log()
                response = None

        return response

    def find_detail_links(self, url):
        """
        Scrape search results page for links to detail pages. Return links.
        """
        response = self.get_response(url)
        if response:
            html_soup = BeautifulSoup(response.text, 'html.parser')
            listing_cards = html_soup.find_all(
                'li', class_='component_property-card js-component_property-card')
            listing_cards = [x for x in listing_cards]
            for listing in listing_cards:
                link = 'https://www.realtor.com' + listing['data-url']
                if not listing.find('div', class_='broker-info').find('span'):
                    self.listing_links.append(link)
                elif listing.find('div', class_='broker-info').find('span').text != 'Provided by':
                    self.listing_links.append(link)
            last_page = False
            if html_soup.find('span', class_='next next-last-page'):
                last_page = True
            print(f'{len(self.listing_links)} total links scraped.')
            return last_page
        else:
            return

    def scrape_details(self):
        """
        Scrape details pages from detail links attribute. Write results to JSON file.
        """
        try:
            # Iterate through detail pages
            for i, link in enumerate(self.listing_links):
                try:
                    response = self.get_proxied_response(link)
                    if not response:
                        continue
                    print_green(
                        f'Successful GET ({i+1}/{len(self.listing_links)})')
                    time.sleep(5)
                    detail_page = BeautifulSoup(response.text, 'html.parser')
                    with open('response.html', 'w') as hf:
                        hf.write(response.text)
                    scripts = detail_page.find_all(
                        'script', type='text/javascript')
                    for s in scripts:
                        if '$.extend(' in str(s):
                            json_data = str(s)
                            json_data = json_data[json_data.find(
                                '{'):json_data.rfind('}')+1]
                    if json_data:
                        data = json.loads(json_data)
                    else:
                        skip_listing(self.college, 'data')
                        continue
                    street_address = data["full_address_display"]
                    photos = [None]
                    if "photos" in data:
                        photos = data["photos"]
                        photos = [photo["url"] for photo in photos]
                    # Iterate through floor plans (if present)
                    if "grouped_floor_plans" in data:
                        floor_plans = data["grouped_floor_plans"]
                        for fp in floor_plans:
                            units = data["grouped_floor_plans"][fp]
                            # Iterate through units in each floor plan
                            for u in units:
                                name = u["name"]
                                if name:
                                    address, unitNum = find_unit_num(name)
                                else:
                                    address, unitNum = find_unit_num(
                                        street_address)
                                price_high = u["price"]
                                beds = u["beds"]
                                baths = u["baths"]
                                sqft = u["sqft"]
                                unit_images = list(
                                    set([u["photo_url"]] + photos))
                                available = u['available_date']
                                # Build document for DB
                                unit = {
                                    'address': address,
                                    'unitNum': unitNum,
                                    'price_high': price_high,
                                    'price_low': None,
                                    'beds': beds,
                                    'baths': baths,
                                    'pets': None,
                                    'sqft': sqft,
                                    'provider': 'Realtor',
                                    'images': [x for x in unit_images if x],
                                    'URL': link,
                                    'original_site': None,
                                    'available': available
                                }
                                write_to_raw_json(unit, self.college)
                    # Otherwise, just record the single unit
                    else:
                        address, unitNum = find_unit_num(street_address)
                        price_high = data["price"]
                        beds = data["beds"]
                        baths = data["baths"]
                        sqft = None
                        if 'sqft' in data:
                            sqft = data["sqft"]
                        unit = {
                            'address': address,
                            'unitNum': unitNum,
                            'price_high': price_high,
                            'price_low': None,
                            'beds': beds,
                            'baths': baths,
                            'pets': None,
                            'sqft': sqft,
                            'provider': 'Realtor',
                            'images': [x for x in photos if x],
                            'URL': link,
                            'original_site': None,
                            'available': None
                        }
                        write_to_raw_json(unit, self.college)

                # Print Scraping errors and write to log file
                except Exception as e:
                    write_to_error_log(self.college, 'Realtor', e, link=link)
                    skip_listing(self.college, 'error')
                    continue

        except Exception as e:
            write_to_error_log(self.college, 'Realtor', e, link=link)

    def get_data(self):
        """
        Initialize scraping process.
        """
        print(f"Realtor.com: {self.city}-{self.state}")
        last_page = False
        page_count = 1
        while not last_page:
            print(f'Getting Page {page_count} Links', end=' - ')
            page_url = f"https://www.realtor.com/apartments/Corvallis_OR/pg-{page_count}"
            last_page = self.find_detail_links(page_url)
            page_count += 1
        print('Finding details')
        self.scrape_details()


# Construct CLI Arguments and parse input
# ap = argparse.ArgumentParser()
# ap.add_argument("-u", "--university", required=True, help="College to Scrape listings for")
# ap.add_argument("-c", "--city", required=True, help="College to Scrape listings for")
# ap.add_argument("-s", "--state", required=True, help="State to Scrape listings for")
# args = vars(ap.parse_args())
# college = args['university']
# city = args['city']
# state = args['state']
# Run Scraper
a = Realtor('corvallis', 'or', '../OSU')
a.get_data()
