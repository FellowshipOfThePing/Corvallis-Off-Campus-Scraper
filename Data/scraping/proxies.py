# Description: Utility functions for scraping via proxies

from selenium.webdriver.chrome.options import Options
from lxml.html import fromstring
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
from scraping.loggers import *
from scraping.utils import *
import scraping.add_features
import scraping.settings
import datetime
import requests
import pprint
import time
import json
import sys
import os



def get_proxies(proxies=None):
    """
    Scrape Proxy IPs from web, into tuples of (IP, 0).
    The 0 in tuple represents # of connection errors per IP. 
    """
    # Access blacklist
    url = 'https://free-proxy-list.net/'
    proxy_blacklist = './proxies/proxy_blacklist.json'
    if os.stat(proxy_blacklist).st_size != 0:
        with open(proxy_blacklist) as blf:
            blacklist = json.load(blf)
    else:
        blacklist = []

    # Get proxies from page
    response = requests.get(url)
    parser = fromstring(response.text)

    # Create proxy list if not appending (see default param)
    if not proxies:
        proxies = set()

    # Add proxies from page
    i = 0
    while len(proxies) < 10:
        p = parser.xpath('//tbody/tr')[i]
        if p.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([p.xpath('.//td[1]/text()')[0], p.xpath('.//td[2]/text()')[0]])
            if proxy not in blacklist:
                proxy = (proxy, 0)
                proxies.add(proxy)
                print(f"Added to Proxies: {proxy[0]}")
        i += 1
    return proxies



def write_to_proxy_log(proxy, provider, decrement=False, link=None, blacklist=False):
    """
    Format and write given proxy failure to proxy Log
    """
    # Open file and find traceback values
    proxy_file = './proxies/proxy_log.json'

    # Open and load proxy Log
    if os.stat(proxy_file).st_size != 0:
        with open(proxy_file) as pfl:
            proxy_log = json.load(pfl)
    else:
        proxy_log = dict()

    # Build entry
    entry = {
        'blacklisted': False,
        'failure_count': proxy[1]
    }
    
    # Record Blacklisting
    if blacklist:
        entry['blacklisted'] = True
        entry['failure_count'] += 1

    if not decrement:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        if exc_type:
            entry['last_call'] = str(exc_type)
        else:
            entry['last_call'] = 'Non-200 GET Response'
    else:
        entry['last_call'] = 'Success'

    # Copy to file
    proxy_log[proxy[0]] = entry
    with open(proxy_file, 'w') as pfl:
        json.dump(proxy_log, pfl, indent=4)



def clear_proxy_log():
    """
    Clears proxy_log.json file in proxies directory.
    """
    open('./proxies/proxy_log.json', 'w').close()



def write_to_proxy_blacklist(proxy):
    """
    Add proxy to blacklist file. Typically called after timeout.
    """
    # Open blacklist file
    proxy_blacklist = f'./proxies/proxy_blacklist.json'
    if os.stat(proxy_blacklist).st_size != 0:
        with open(proxy_blacklist) as pbl:
            blacklist = set(json.load(pbl))
    else:
        blacklist = set()

    # Copy proxy IP to list
    blacklist.add(proxy[0])

    # Save back to file
    with open(proxy_blacklist, 'w') as pbl:
        json.dump(list(blacklist), pbl, indent=4)



def clear_proxy_blacklist():
    """
    Clears proxy_blacklist.json file in proxies directory
    """
    open('./proxies/proxy_blacklist.json', 'w').close()



def increment_proxy_error(error_type):
    """
    Write given input to proxy_stats.json log - found in proxies folder.
    """
    proxy_stats = './proxies/proxy_stats.json'
    with open(proxy_stats) as ps:
        stats = dict(json.load(ps))

    stats['errors'][error_type] += 1

    with open(proxy_stats, 'w') as ps:
        json.dump(stats, ps, indent=4)



def adjust_proxy_mean_GET_success(start_time):
    """
    Adjust mean_GET_success_time in proxy_stats.json.
    """
    # End Timer
    end_time = time.time()
    total_time = round(end_time - start_time, 2)

    # Get log from file
    proxy_stats = './proxies/proxy_stats.json'
    with open(proxy_stats) as ps:
        stats = dict(json.load(ps))

    # Adjust mean success time stat
    times = stats['stats']["GET_successes"]
    mean = stats['stats']["mean_GET_success_time"]
    new_mean = ((times * mean) + total_time) / (times + 1)
    stats['stats']["GET_successes"] += 1
    stats['stats']["mean_GET_success_time"] = round(new_mean, 2)

    with open(proxy_stats, 'w') as ps:
        json.dump(stats, ps, indent=4)



def adjust_proxy_mean_GET_failure(start_time):
    """
    Adjust mean_GET_failure_time in proxy_stats.json.
    """
    # End timer
    end_time = time.time()
    total_time = round(end_time - start_time, 2)

    # Get log from file
    proxy_stats = './proxies/proxy_stats.json'
    with open(proxy_stats) as ps:
        stats = dict(json.load(ps))

    # Adjust mean failure time stat
    times = stats['stats']["GET_failures"]
    mean = stats['stats']["mean_GET_failure_time"]
    new_mean = ((times * mean) + total_time) / (times + 1)
    stats['stats']["GET_failures"] += 1
    stats['stats']["mean_GET_failure_time"] = round(new_mean, 2)

    with open(proxy_stats, 'w') as ps:
        json.dump(stats, ps, indent=4)



def clear_proxy_stats():
    """
    Write default stats dict to proxy_stats.json file.
    """
    default = {
        "errors": {
            "ContentEncoding": 0,
            "SSL": 0,
            "Timeout": 0,
            "Proxy": 0,
            "Other": 0
        },
        "stats": {
            "GET_successes": 0,
            "mean_GET_success_time": 0,
            "GET_failures": 0,
            "mean_GET_failure_time": 0,
            "num_blacklisted": 0
        }
    }
    with open('./proxies/proxy_stats.json', 'w') as ps:
        json.dump(default, ps, indent=4)