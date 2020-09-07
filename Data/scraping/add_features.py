# Description: Functions for adding/updating feature values to DB collections
# Last Update: 04/11/20
# Update Desc: Created

# TODO: Update add_feature function to be flexible (right now it only really works with adding the driving features)
# TODO: Create refresh_db that pulls down current db, checks each document to see what it does/doesn't have, fills in the blanks, and sends back
# TODO: Maybe see if there is a more refined way of checking distance to campus edges - this has to be scalable and automatable


from pymongo import MongoClient
from scraping.loggers import *
from pprint import pprint
from scraping.utils import *
from scraping.loggers import *
import scraping.settings
import requests
import json
import math
import time
import os


BING_KEY = os.getenv("bing_key")
MONGO_KEY = os.getenv("mongo_key")
CAMPUS_CENTER = (44.5650, -123.2789)


def get_coords_from_address(state, city, address):
    """
    Address param is a dictionary with location data set to null
    """
    # Get Apartment Coordinates
    params = {
        "CountryRegion": 'US',
        "adminDistrict": state,
        "locality": city,
        "addressLine": address,
        "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
    }
    url = "http://dev.virtualearth.net/REST/v1/Locations"
    response = requests.get(url, params=params)

    apartment_coords = response.json()["resourceSets"][0]['resources'][0]['point']['coordinates']
    address['latitude'] = apartment_coords[0]
    address['longitude'] = apartment_coords[1]
    print('Coordinates Call From API:', address, apartment_coords)

    return address



def get_coords_from_addresses(state, city, addresses):
    """
    Addresses param is a dictionary with address keys and values of empty dicts
    """
    # Get Apartment Coordinates
    num_addresses = len(addresses)
    for i, address in enumerate(addresses):
        if ('latitude' not in addresses[address]) or ('longitude' not in addresses[address]):
            params = {
                "CountryRegion": 'US',
                "adminDistrict": state,
                "locality": city,
                "addressLine": address,
                "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
            }
            url = "http://dev.virtualearth.net/REST/v1/Locations"
            response = requests.get(url, params=params)

            apartment_coords = response.json()["resourceSets"][0]['resources'][0]['point']['coordinates']
            addresses[address]['latitude'] = apartment_coords[0]
            addresses[address]['longitude'] = apartment_coords[1]
            print(f'{i+1}/{num_addresses}', 'Coordinates Found:', address, apartment_coords)

    return addresses



def get_walk_data(addresses):
    """
    Addresses param is a dictionary with address keys and values of dictionaries with distances data.
    """

    # For each address, find the distance to campus center
    num_addresses = len(addresses)
    for i, address in enumerate(addresses):
        if ('walk_to_campus_miles' not in addresses[address]) or ('walk_to_campus_minutes' not in addresses[address]):
            params = {
                "origins": str(addresses[address]['latitude']) + ',' + str(addresses[address]['longitude']),
                "destinations": str(CAMPUS_CENTER[0]) + ',' + str(CAMPUS_CENTER[1]),
                "timeUnit": "minute",
                "distanceUnit": "mile",
                "travelMode": "walking",
                "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
            }
            url = "https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix"
            response = requests.get(url, params=params)

            # Find minimum distance, add to addresses
            travelResults = response.json()['resourceSets'][0]['resources'][0]['results']
            shortestDistance = min(travelResults, key = lambda x: x['travelDistance'])
            distance = shortestDistance['travelDistance']
            duration = shortestDistance['travelDuration']
            addresses[address]['walk_to_campus_miles'] = distance
            addresses[address]['walk_to_campus_minutes'] = duration
            print(f'{i+1}/{num_addresses}', 'Walk Distances Found:', address, distance)

    return addresses



def get_drive_data(addresses):
    """
    Addresses param is a dictionary with address keys and values of dictionaries with distances data.
    """

    # For each address, find the distance to campus center
    num_addresses = len(addresses)
    for i, address in enumerate(addresses):
        if ('drive_to_campus_miles' not in addresses[address]) or ('drive_to_campus_minutes' not in addresses[address]):
            params = {
                "origins": str(addresses[address]['latitude']) + ',' + str(addresses[address]['longitude']),
                "destinations": str(CAMPUS_CENTER[0]) + ',' + str(CAMPUS_CENTER[1]),
                "timeUnit": "minute",
                "distanceUnit": "mile",
                "travelMode": "driving",
                "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
            }
            url = "https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix"
            response = requests.get(url, params=params)

            # Find minimum distance, add to addresses
            travelResults = response.json()['resourceSets'][0]['resources'][0]['results']
            shortestDistance = min(travelResults, key = lambda x: x['travelDistance'])
            distance = shortestDistance['travelDistance']
            duration = shortestDistance['travelDuration']
            addresses[address]['drive_to_campus_miles'] = distance
            addresses[address]['drive_to_campus_minutes'] = duration
            print(f'{i+1}/{num_addresses}', 'Drive Distances Found', address, distance)

    return addresses



def get_distance_data(addresses, city, state):
    """
    Calls get_drive_data/get_walk_data, and adds to given listings dictionary.
    """
    print(len(addresses), 'Unique Addresses Found')
    addresses = get_coords_from_addresses(state, city, addresses)
    addresses = get_walk_data(addresses)
    addresses = get_drive_data(addresses)

    return addresses



def add_driving_data_to_docs(documents):
    """
    Add driving data to documents, independant of listings scraping process.
    Documents param is a list of documents that mirrors a collection in the DB.
    Returns documents, modified with new driving data.
    """
    # Dictionary of addresses (removes duplicates before calling API)
    addresses = {doc['address']:doc for doc in documents}

    # For each address, find the distance to campus center
    for i, address in enumerate(addresses):
        if 'drive_to_campus_miles' in addresses[address]:
            continue
        params = {
            "origins": str(addresses[address]['latitude']) + ',' + str(addresses[address]['longitude']),
            "destinations": str(CAMPUS_CENTER[0]) + ',' + str(CAMPUS_CENTER[1]),
            "timeUnit": "minute",
            "distanceUnit": "mile",
            "travelMode": "driving",
            "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
        }
        url = "https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix"
        response = requests.get(url, params=params)

        # Find minimum distance, add to addresses
        travelResults = response.json()['resourceSets'][0]['resources'][0]['results']
        shortestDistance = min(travelResults, key = lambda x: x['travelDistance'])
        distance = shortestDistance['travelDistance']
        duration = shortestDistance['travelDuration']
        addresses[address]['drive_to_campus_miles'] = distance
        addresses[address]['drive_to_campus_minutes'] = duration
        print(i+1, 'Driving Distances Found', address, distance)

    # Add new values to documents before returning
    for doc in documents:
        doc['drive_to_campus_miles'] = addresses[doc['address']]['drive_to_campus_miles']
        doc['drive_to_campus_minutes'] = addresses[doc['address']]['drive_to_campus_minutes']
        pprint.pprint(doc, indent=2)

    return documents



def add_walking_data_to_docs(documents):
    """
    Add walking data to documents, independant of listings scraping process.
    Documents param is a list of documents that mirrors a collection in the DB.
    Returns documents, modified with new walking data.
    """
    # Dictionary of addresses (removes duplicates before calling API)
    addresses = {doc['address']:doc for doc in documents}

    # For each address, find the distance to campus center
    for i, address in enumerate(addresses):
        if 'walk_to_campus_miles' in addresses[address]:
            continue
        params = {
            "origins": str(addresses[address]['latitude']) + ',' + str(addresses[address]['longitude']),
            "destinations": str(CAMPUS_CENTER[0]) + ',' + str(CAMPUS_CENTER[1]),
            "timeUnit": "minute",
            "distanceUnit": "mile",
            "travelMode": "walking",
            "key": "Ak5e3z6SYjf7-teRSZQ2aBVTMA2izoerpnJQX_1df0MT0_bEILytK1LOwv7Kg7tU"
        }
        url = "https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix"
        response = requests.get(url, params=params)

        # Find minimum distance, add to addresses
        travelResults = response.json()['resourceSets'][0]['resources'][0]['results']
        shortestDistance = min(travelResults, key = lambda x: x['travelDistance'])
        distance = shortestDistance['travelDistance']
        duration = shortestDistance['travelDuration']
        addresses[address]['walk_to_campus_miles'] = distance
        addresses[address]['walk_to_campus_minutes'] = duration
        print(i+1, 'Walking Distances Found', address, distance)

    # Add new values to documents before returning
    for doc in documents:
        doc['walk_to_campus_miles'] = addresses[doc['address']]['walk_to_campus_miles']
        doc['walk_to_campus_minutes'] = addresses[doc['address']]['walk_to_campus_minutes']
        pprint.pprint(doc, indent=2)

    return documents



def backup_distances_to_local(collection):
    """
    Download documents from remote collection and store location data locally.
    """
    # Copy docs from collection to dictionary of dictionaries
    documents = collection.find()

    # Load location_data.json
    file_name = './backups/location_data.json'
    if os.stat(file_name).st_size != 0:
        with open(file_name) as loc_file:
            loc_data = json.load(loc_file)
    else:
        loc_data = dict()

    # Grab location data from collection documents
    for document in documents:
        loc_vals = {
            "latitude": document["latitude"],
            "longitude": document["longitude"],
            "walk_to_campus_miles": document["walk_to_campus_miles"],
            "walk_to_campus_minutes": document["walk_to_campus_minutes"],
            "drive_to_campus_miles": document["drive_to_campus_miles"],
            "drive_to_campus_minutes": document["drive_to_campus_minutes"]
        }
        loc_data[document['address']] = loc_vals
    
    # Save location data back to local file
    with open(file_name, 'w') as loc_file:
        json.dump(loc_data, loc_file, indent=4)
