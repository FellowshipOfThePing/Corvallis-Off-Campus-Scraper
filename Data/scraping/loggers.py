# Description: Functions for writing to log files

# TODO: Rewrite archive saver so that it gets archive from DB, not local file.

from selenium.webdriver.chrome.options import Options
from lxml.html import fromstring
from pymongo import MongoClient
from selenium import webdriver
from scraping.proxies import *
from bs4 import BeautifulSoup
import scraping.add_features
from scraping.utils import *
import scraping.settings
import traceback
import datetime
import requests
import pprint
import time
import json
import sys
import os


def remove_duplicate_images(images):
    """
    Remove all duplicate URLs from images array, return new list.
    """
    seen = set()
    unique_images = []
    for image in images:
        if image not in seen:
            unique_images.append(image)
            seen.add(image)
    return unique_images


def write_to_raw_json(unit, college):
    """
    Write given listing to raw_listings file. Typically called during the scraping process.
    """
    # Read from raw_listings.json
    file_name = f"{college}/logs/raw_listings.json"
    if os.stat(file_name).st_size != 0:
        with open(file_name) as jf:
            raw_listings = json.load(jf)
    else:
        raw_listings = dict()

    # Modify raw_listings with new unit
    raw_listing_id = create_duplicate_id(unit, unit_id=True)

    # If unit not found yet
    if raw_listing_id not in raw_listings:
        unit['images'] = remove_duplicate_images(unit['images'])
        raw_listings[raw_listing_id] = unit
        raw_listings[raw_listing_id]['duplicates'] = []
        raw_listings[raw_listing_id]['raw_id'] = raw_listing_id
        update_scrape_stats(college, unit)
        print('Listing Added to JSON:', raw_listing_id)

    # If unit already found from different provider
    elif unit['provider'] != raw_listings[raw_listing_id]['provider']:
        raw_listings[raw_listing_id]['images'] += unit['images']
        raw_listings[raw_listing_id]['images'] = remove_duplicate_images(
            raw_listings[raw_listing_id]['images'])
        dup_unit = {
            'provider': unit['provider'],
            'URL': unit['URL']
        }
        raw_listings[raw_listing_id]['duplicates'].append(dup_unit)
        update_scrape_stats(college, unit, duplicate=True)
        print('Duplicate Added to JSON:', raw_listing_id)

    # Else, don't do anything

    # Write back to file
    with open(file_name, 'w') as jf:
        json.dump(raw_listings, jf, indent=4)


def write_to_formatted_json(unit, college):
    """
    Write given listing to formatted_listings file. Typically called after the scraping process.
    """
    # Read from formatted_listings.json
    file_name = f"{college}/logs/formatted_listings.json"
    if os.stat(file_name).st_size != 0:
        with open(file_name) as jf:
            listings = json.load(jf)
    else:
        listings = dict()

    listing_id = create_duplicate_id(unit, unit_id=False)

    # If identical listing not already found
    if listing_id not in listings:
        listings[listing_id] = unit
        listings[listing_id]['formatted_id'] = listing_id
        listings[listing_id]['units'] = dict()
        listings[listing_id]['sources'] = dict()

    # If identical listing already found
    else:
        # find number of units in original listing
        og_num_units = len(listings[listing_id]['units'])
        # if no units in original listing
        if og_num_units == 0:
            # find unitNum of original
            og_unit_num = listings[listing_id]['unitNum']
            # If null, set to default
            if not og_unit_num:
                og_unit_num = 'defaultUnit0'
            # Capture OG's provider, URL, and availability, add to 'units' dict as a unit
            listings[listing_id]['units'][og_unit_num] = {
                "provider": listings[listing_id]['provider'],
                "URL": listings[listing_id]['URL'],
                "available": listings[listing_id]['available']
            }
            listings[listing_id]['unitNum'] = None
        # Check if same unit already in units dict
        if unit['unitNum'] in listings[listing_id]['units']:
            listings[listing_id]['duplicates'].append(unit)
        # if not, capture new listings provider, URL, and availability, add to 'units' dict as a unit
        else:
            new_unit_num = unit['unitNum']
            if not new_unit_num:
                new_unit_num = f"defaultUnit{len(listings[listing_id]['units'])}"
            listings[listing_id]['units'][new_unit_num] = {
                "provider": unit['provider'],
                "URL": unit['URL'],
                "available": unit['available']
            }
        # Throw images of new unit into OG listing, and cut duplicates
        listings[listing_id]['images'] += unit['images']
        listings[listing_id]['images'] = remove_duplicate_images(
            listings[listing_id]['images'])

    # Finally, adjust listings 'sources' dict to reflect sources
    if unit['provider'] not in listings[listing_id]['sources']:
        listings[listing_id]['sources'][unit['provider']] = {
            'link': unit['URL'],
            'numUnits': 1
        }
    # If provider already present, just increment numUnits
    else:
        listings[listing_id]['sources'][unit['provider']]['numUnits'] += 1

    print('Listing Added to Formatted JSON:', listing_id)

    # Write back to file
    with open(file_name, 'w') as jf:
        json.dump(listings, jf, indent=4)


def write_to_address_json(unit, college):
    """
    Write given listing to address_listings file. Typically called after the scraping process.
    """
    file_name = f"{college}/logs/address_listings.json"
    if os.stat(file_name).st_size != 0:
        with open(file_name) as jf:
            listings = json.load(jf)
    else:
        listings = dict()

    # Create address_id
    address_id = ''.join((unit['address'].upper()).split())

    # If not already found
    if address_id not in listings:
        listings[address_id] = unit
        listings[address_id]['address_id'] = address_id
        listings[address_id]['units'] = dict()
        listings[address_id]['sources'] = dict()
        print('Listing Added to Addressed JSON:', address_id)

    # If already found
    else:
        # find number of units in original listing
        og_num_units = len(listings[address_id]['units'])
        # if no units in original listing
        if og_num_units == 0:
            # find unitNum of original
            og_unit_num = listings[address_id]['unitNum']
            # If null, set to default
            if not og_unit_num:
                og_unit_num = 'defaultUnit0'
            # Capture OG's provider, URL, and availability, add to 'units' dict as a unit
            listings[address_id]['units'][og_unit_num] = {
                "price_high": listings[address_id]['price_high'],
                "price_low": listings[address_id]['price_low'],
                "beds": listings[address_id]['beds'],
                "baths": listings[address_id]['baths'],
                "sqft": listings[address_id]['sqft'],
                "provider": listings[address_id]['provider'],
                "URL": listings[address_id]['URL'],
                "available": listings[address_id]['available']
            }
            listings[address_id]['unitNum'] = None
        # Check if same unit already in units dict
        if unit['unitNum'] in listings[address_id]['units']:
            listings[address_id]['duplicates'].append(unit)
        # if not, capture new listings provider, URL, and availability, add to 'units' dict as a unit
        else:
            new_unit_num = unit['unitNum']
            if not new_unit_num:
                new_unit_num = f"defaultUnit{len(listings[address_id]['units'])}"
            listings[address_id]['units'][new_unit_num] = {
                "price_high": unit['price_high'],
                "price_low": unit['price_low'],
                "beds": unit['beds'],
                "baths": unit['baths'],
                "sqft": unit['sqft'],
                "provider": unit['provider'],
                "URL": unit['URL'],
                "available": unit['available']
            }
        # Throw images of new unit into OG listing, and cut duplicates
        listings[address_id]['images'] += unit['images']
        listings[address_id]['images'] = remove_duplicate_images(
            listings[address_id]['images'])

    # Finally, adjust listings 'sources' dict to reflect sources
    if unit['provider'] not in listings[address_id]['sources']:
        listings[address_id]['sources'][unit['provider']] = {
            'link': unit['URL'],
            'numUnits': 1
        }
    # If provider already present, just increment numUnits
    else:
        listings[address_id]['sources'][unit['provider']]['numUnits'] += 1

    # Write back to file
    with open(file_name, 'w') as jf:
        json.dump(listings, jf, indent=4)


def write_to_error_log(college, provider, e, link=None):
    """
    Format and write given error to Error Log
    """
    # Find traceback values
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    error_file = f'{college}/logs/error_log.json'

    # Open and load Error Log
    if os.stat(error_file).st_size != 0:
        with open(error_file) as el:
            error_log = json.load(el)
    else:
        error_log = []

    # Build entry
    entry = {
        'fileName': fname,
        'errorText': str(e),
        'errorType': str(exc_type),
        'line-number': exc_tb.tb_lineno,
        'provider': provider,
        'link': link
    }

    # Copy to file
    error_log.append(entry)
    with open(error_file, 'w') as el:
        json.dump(error_log, el, indent=4)


def write_to_skipped_listings(college, provider, reason, address):
    """
    Write skipped listing to log for later inspection.
    """
    # Find filepath
    log_file = f'{college}/logs/skipped_listings.json'

    # Open and load Log
    if os.stat(log_file).st_size != 0:
        with open(log_file) as lf:
            log = json.load(lf)
    else:
        log = []

    # Build entry
    entry = {
        'provider': provider,
        'reasonForSkip': reason,
        'address': address
    }

    # Copy to file
    log.append(entry)
    print('Listing Skipped:', reason)
    with open(log_file, 'w') as lf:
        json.dump(log, lf, indent=4)


def write_to_duplicate_log(unit, unit_id):
    """
    Write to duplicate log (list of dictionaries)
    """
    file_name = "./logs/duplicates.json"
    if os.stat(file_name).st_size != 0:
        with open(file_name) as jf:
            duplicates = json.load(jf)
    else:
        duplicates = []

    unit['duplicate_id'] = unit_id
    duplicates.append(unit)

    with open(file_name, 'w') as jf:
        json.dump(duplicates, jf, indent=4)


def clear_scrape_log(college):
    """
    Clear school-specific scrape stats log.
    """
    stats = {
        "total_listings_scraped": 0,
        "unique_listings_scraped": 0,
        "duplicates_scraped": 0,
        "new_listings": 0,
        "retired_listings": 0,
        "total_skipped": 0,
        "skipped_by_type": {
            "insufficient_data": 0,
            "wrong_city": 0,
            "unavailable": 0,
            "error": 0
        },
        "total_null": 0,
        "null_by_type": {
            "address": 0,
            "unitNum": 0,
            "price_high": 0,
            "price_low": 0,
            "beds": 0,
            "baths": 0,
            "pets": 0,
            "sqft": 0,
            "provider": 0,
            "images": 0,
            "URL": 0,
            "available": 0,
            "original_site": 0
        },
        "skipped_scrapers": [],
        "providers": {},
    }
    with open(f"./{college}/logs/stats_log.json", 'w') as sl:
        json.dump(stats, sl, indent=4)


def update_scrape_stats(college, unit, duplicate=False):
    """
    Update fields in school-specific scrape stats log.
    """
    # Grab stats dict
    log_file = f"./{college}/logs/stats_log.json"
    with open(log_file) as sl:
        stats = json.load(sl)

    # Check for provider entry
    if unit['provider'] not in stats["providers"]:
        stats = add_provider(stats, unit['provider'])

    # Increment listing totals
    stats["total_listings_scraped"] += 1
    stats["providers"][unit['provider']]["total_listings_scraped"] += 1
    if duplicate:
        stats["duplicates_scraped"] += 1
        stats["providers"][unit['provider']]["duplicates_scraped"] += 1
    else:
        stats["unique_listings_scraped"] += 1
        stats["providers"][unit['provider']]["unique_listings_scraped"] += 1

    # Increment null counters
    for key in unit:
        if unit[key] is None:
            stats["total_null"] += 1
            stats["null_by_type"][key] += 1
            stats["providers"][unit['provider']]["total_null"] += 1
            stats["providers"][unit['provider']]["null_by_type"][key] += 1

    # Write back to file
    with open(log_file, 'w') as sl:
        json.dump(stats, sl, indent=4)


def skip_listing(college, reason, provider):
    """
    Update skipped_listings fields in scrape stats log.
    """
    # Grab stats dict
    log_file = f"./{college}/logs/stats_log.json"
    with open(log_file) as sl:
        stats = json.load(sl)

    # Increment total
    stats['total_skipped'] += 1

    # Check for provider entry
    if provider not in stats["providers"]:
        stats = add_provider(stats, provider)

    # Increment reason total
    reason = reason.lower()
    stats["providers"][provider]["total_skipped"] += 1
    if reason == 'data':
        stats["providers"][provider]["skipped_by_type"]["insufficient_data"] += 1
        stats["skipped_by_type"]["insufficient_data"] += 1
        print_yellow(f'Listing Skipped: Insufficient data')
    elif reason == 'city':
        stats["providers"][provider]["skipped_by_type"]["wrong_city"] += 1
        stats["skipped_by_type"]["wrong_city"] += 1
        print_yellow(f'Listing Skipped: Wrong City')
    elif reason == 'unavailable':
        stats["providers"][provider]["skipped_by_type"]["unavailable"] += 1
        stats["skipped_by_type"]["unavailable"] += 1
        print_yellow(f'Listing Skipped: Unavailable')
    elif reason == 'error':
        stats["providers"][provider]["skipped_by_type"]["error"] += 1
        stats["skipped_by_type"]["error"] += 1
        print_yellow(f'Listing Skipped: Scraping Error')
    else:
        print_red(reason)

    # Write back to file
    with open(log_file, 'w') as sl:
        json.dump(stats, sl, indent=4)


def skip_scraper(college, provider):
    """
    Update skipped_scrapers field in scrape stats log.
    """
    # Grab stats dict
    log_file = f"./{college}/logs/stats_log.json"
    with open(log_file) as sl:
        stats = json.load(sl)

    # Add provider to list
    stats["skipped_scrapers"].append(provider)

    # Write back to file
    with open(log_file, 'w') as sl:
        json.dump(stats, sl, indent=4)

    # Print notice
    print_red('Skipping Scraper...')


def update_new_ret_stats(college, new, retired, total):
    """
    Update new/retired listings stats in school-specific stats log.
    """
    # Open file, grab stats
    log_file = f"./{college}/logs/stats_log.json"
    with open(log_file) as sl:
        stats = json.load(sl)

    # Update total stats
    stats["new_listings"] = len(new)
    stats["retired_listings"] = len(retired)

    # Update stats by provider
    for listing in new:
        stats["providers"][new[listing]['provider']]["new_listings"] += 1

    for listing in retired:
        if retired[listing]['provider'] in stats["providers"]:
            stats["providers"][retired[listing]
                               ['provider']]["retired_listings"] += 1

    # Write back to file
    with open(log_file, 'w') as sl:
        json.dump(stats, sl, indent=4)

    # Print Stats to Console and return
    print(f'New Listings: {len(new)}')
    print(f'Retired Listings: {len(retired)}')
    print(f'Total Listings: {total}')


def archive_scrape_stats(college, database):
    """
    Append scrape-stats log to school-specific archive file.
    """
    # Open most recent scrape stats
    log_file = f"./{college}/logs/stats_log.json"
    with open(log_file) as sl:
        stats = json.load(sl)

    # Open archive file, grab stats
    archive_file = f"./{college}/backups/scrape_archive.json"
    if os.stat(archive_file).st_size != 0:
        with open(archive_file) as af:
            archive = json.load(af)
    else:
        archive = dict()

    # Append recent scrape to archive
    current_time = str(datetime.datetime.now())
    current_time = current_time[:current_time.find('.')]
    archive[current_time] = stats

    # Write archive back to file
    with open(archive_file, 'w') as af:
        json.dump(archive, af, indent=4)

    # Overwrite remote collection
    remote_collection = database.scrape_stats_archive
    remote_collection.drop()
    remote_collection.insert_one(archive)


def add_provider(stats, provider):
    """
    Add entry to 'providers' section of stats log
    """
    # Check for provider entry
    stats["providers"][provider] = {
        "total_listings_scraped": 0,
        "unique_listings_scraped": 0,
        "duplicates_scraped": 0,
        "new_listings": 0,
        "retired_listings": 0,
        "total_skipped": 0,
        "skipped_by_type": {
            "insufficient_data": 0,
            "wrong_city": 0,
            "unavailable": 0,
            "error": 0
        },
        "total_null": 0,
        "null_by_type": {
            "address": 0,
            "unitNum": 0,
            "price_high": 0,
            "price_low": 0,
            "beds": 0,
            "baths": 0,
            "pets": 0,
            "sqft": 0,
            "provider": 0,
            "images": 0,
            "URL": 0,
            "available": 0,
            "original_site": 0
        }
    }
    return stats
