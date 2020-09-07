# Description: 'Trigger' Script for scrapers. Logs scraped data in local JSON files, compares listings,
#              clears listings db collection, and refills with refined JSON file.

# TODO: De-hard code get_distance_data from just tracking Corvallis
# TODO: Remember to TIME everything
# TODO: Centralize appfolio json files so that they don't have to be duplicated for each college folder. Just sort them by college in central files.
# TODO: After you have this loaded up in a cloud cron routine, write a program that emails you the scrape stats file after every time it scrapes
# TODO: Track everything! This includes API calls of all sorts. (distance data especially). It would be nice to be able to see that in real time.
# TODO: Change raw logger so that images don't get turned into sets, but still deal with duplicates
# TODO: Consider more exception handling on THIS page. If an automated scrape stops in the middle of the night, we want it to keep trying/skip whatever is stopping it. Or at least notify us of the error through an email.
# TODO: edit address-based logger. We don't need most of that info.
# TODO: Seems like this scrapes inconsistent data, so maybe we should set up the CRON job so that it only removes listings from the DB once per week, but adds to it each night? Something like that.


from bson.objectid import ObjectId
from scraping.add_features import *
from pymongo import MongoClient
from scraping.loggers import *
from scraping.utils import *
import scraping.settings
import datetime
import json
import sys
import os



class Scraper:
    """
    Run scrapers and store data in folders corresponding to given city name
    """
    def __init__(self, college, cities, state):
        """
        Initialize attributes.
        """
        self.db = MongoClient(os.getenv("mongo_key"))[college]
        self.college = college
        self.cities = [c.lower() for c in cities]
        self.state = state.lower()
        self.raw_listings_json = f'{college}/logs/raw_listings.json'
        self.raw_backup = f'{college}/backups/raw_listings_backup.json'
        self.formatted_listings_json = f'{college}/logs/formatted_listings.json'
        self.formatted_backup = f'{college}/backups/formatted_listings_backup.json'
        self.address_listings_json = f'{college}/logs/address_listings.json'
        self.address_backup = f'{college}/backups/address_listings_backup.json'
        self.location_data = f'{college}/backups/location_data.json'
        self.error_log = f'{college}/logs/error_log.json'
        self.stats_log = f'{college}/logs/stats_log.json'
        self.scraper_path = f'{college}/scrapers'
        

    def prepare_logs(self):
        """
        Init/Clear all JSON logs.
        """
        print("\n********** PREPARING LOGS **********\n")

        # Create log/backup file paths if not already present
        if not os.path.exists(f'{self.college}/logs/'):
            os.makedirs(f'{self.college}/logs/')
        if not os.path.exists(f'{self.college}/backups/'):
            os.makedirs(f'{self.college}/backups/')
            open(self.raw_backup, 'w+').close()
            open(self.formatted_backup, 'w+').close()
            open(self.address_backup, 'w+').close()
            open(self.location_data, 'w+').close()
            open(f'{self.college}/backups/scrape_archive.json', 'w+').close()

        # Clear Logs
        print('Clearing Error Log...')
        open(self.error_log, 'w+').close()
        print('Clearing Stats Log...')
        clear_scrape_log(self.college)
        print('Clearing Raw Listings Log...')
        open(self.raw_listings_json, 'w+').close()
        print('Clearing Formatted Listings Log...')
        open(self.formatted_listings_json, 'w+').close()
        print('Clearing Addressed Listings Log...')
        open(self.address_listings_json, 'w+').close()


    def scrape(self):
        """
        Call Scraper files
        """
        print("\n********** SCRAPING SITES **********\n")
        # Aggregator Scrapers
        for city in self.cities:
            for filename in os.listdir('aggregators'):
                try:
                    command_string = f'python ./aggregators/{filename} --university ./{self.college} --city {city} --state {self.state}'
                    os.system(command_string)
                except Exception as e:
                    write_to_error_log(self.college, filename, e)
                    skip_scraper(self.college, filename + ' - Scraper Threw Exception')
                    continue

        # Template Scrapers
        for city in self.cities:
            for filename in os.listdir('templates'):
                try:
                    command_string = f'python ./templates/{filename} --university ./{self.college} --city {city} --state {self.state}'
                    os.system(command_string)
                except Exception as e:
                    write_to_error_log(self.college, filename, e)
                    skip_scraper(self.college, filename + ' - Scraper Threw Exception')
                    continue

        # Local Scrapers
        for filename in os.listdir(self.scraper_path):
            try:
                os.system(f'python ./{self.scraper_path}/{filename}')
            except Exception as e:
                write_to_error_log(self.college, filename, e)
                skip_scraper(self.college, filename + ' - Scraper Threw Exception')
                continue


    def update_location_data(self):
        """
        Update location data for raw_listings.json
        """
        print("\n********** UPDATING LOCATION DATA **********\n")

        # Create local location data file (if not already created)
        if not os.path.exists(self.location_data):
            open(self.location_data, 'w+').close()

        # Get data from local raw file
        if os.stat(self.raw_listings_json).st_size != 0:
            with open(self.raw_listings_json) as rl:
                raw_listings = json.load(rl)
        else:
            raw_listings = dict()

        # Get data from remote location data file
        location_data = get_collection_as_dict(self.db.location_data, location=True)

        # Get Addresses from local raw_listings
        addresses = [raw_listings[listing]['address'] for listing in raw_listings]

        # Add addresses to location data
        for address in addresses:
            if address not in location_data:
                location_data[address] = dict()

        # Update location data with API
        location_data = get_distance_data(location_data, 'Corvallis', self.state)

        # Update listings with new location data
        for listing in raw_listings:
            address = raw_listings[listing]['address']
            raw_listings[listing]['latitude'] = location_data[address]['latitude']
            raw_listings[listing]['longitude'] = location_data[address]['longitude']
            raw_listings[listing]['walk_to_campus_miles'] = location_data[address]['walk_to_campus_miles']
            raw_listings[listing]['walk_to_campus_minutes'] = location_data[address]['walk_to_campus_minutes']
            raw_listings[listing]['drive_to_campus_miles'] = location_data[address]['drive_to_campus_miles']
            raw_listings[listing]['drive_to_campus_minutes'] = location_data[address]['drive_to_campus_minutes']

        # Save location data and raw listings to file
        with open(self.raw_listings_json, 'w') as raw, open(self.location_data, 'w') as locations:
            json.dump(raw_listings, raw, indent=4)
            json.dump(location_data, locations, indent=4)

        # Turn location_data into list and save to DB
        location_list = []
        for address in location_data:
            loc = location_data[address]
            loc['address'] = address
            location_list.append(loc)
        self.db.location_data.drop()
        result = self.db.location_data.insert_many(location_list)
        print('DB Location Data Updated')


    def compare_raw_listings(self):
        """
        Compare raw_listings.json with raw_listings collection.
        """
        print("\n********** COMPARING RAW LISTINGS **********\n")

        # Get raw_listings from database
        raw_listings_collection = get_collection_as_dict(self.db.raw_listings, raw=True)

        # Get newly-scraped raw-listings from local JSON
        if os.stat(self.raw_listings_json).st_size != 0:
            with open(self.raw_listings_json, 'r') as raw:
                raw_listings_json = json.load(raw)
        else:
            with open(self.raw_listings_json, 'w') as raw:
                raw_listings_json = dict()
                json.dump(raw_listings_json, raw, indent=4)
            
        # ---- Find differences between previous and new listings ---- #
        
        # Dict of newly scraped listings (not found in remote collection)
        new_listings = {rl:raw_listings_json[rl] for rl in raw_listings_json if rl not in raw_listings_collection}

        # Dict of retired listings (found in collection but not new scrape)
        retired_listings = {rl:raw_listings_collection[rl] for rl in raw_listings_collection if rl not in raw_listings_json}

        # Calculate, log, and return scrape stats
        num_total = (len(raw_listings_collection) + len(new_listings) - len(retired_listings))
        update_new_ret_stats(self.college, new_listings, retired_listings, num_total)
        archive_scrape_stats(self.college, self.db)
        return new_listings, retired_listings


    def update_raw_listings(self, new_listings, retired_listings):
        """
        Update raw listings collection to match newest scrape.
        """
        print("\n********** UPDATING RAW LISTINGS **********\n")
        
        # Get raw_listings from database as dictionary
        raw_listings_collection = get_collection_as_dict(self.db.raw_listings, raw=True)

        # Store connection to remote collection
        remote_collection = self.db.raw_listings

        # Delete retired listings from remote collection
        for rl in retired_listings:
            deleted_id = retired_listings[rl]['_id']
            deleted = remote_collection.delete_one({'_id': ObjectId(raw_listings_collection[rl]['_id'])})
            print(f"- Listing {deleted_id}: {retired_listings[rl]['address']}")

        # Add new_listings to remote collection
        for nl in new_listings:
            result = remote_collection.insert_one(new_listings[nl])
            print(f"+ Listing {result.inserted_id}: {new_listings[nl]['address']}")


    def backup_raw_listings(self):
        """
        Update local and remote archives with new remote raw_listings collection
        """
        print("\n********** BACKING UP RAW LISTINGS **********\n")

        # Get remote listings collection as list
        remote_listings = get_collection_as_dict(self.db.raw_listings, raw=True)

        # Read from local file
        if os.stat(self.raw_backup).st_size != 0:
            with open(self.raw_backup) as rb:
                local_raw_backup = json.load(rb)
        else:
            local_raw_backup = dict()

        # Update local backup
        for listing in remote_listings:
            local_raw_backup[listing] = remote_listings[listing]
            local_raw_backup[listing]['_id'] = str(local_raw_backup[listing]['_id'])
            local_raw_backup[listing]['last_scraped'] = str(datetime.datetime.now())

        # Save local backup
        with open(self.raw_backup, 'w') as rb:
            json.dump(local_raw_backup, rb, indent=4)

        # Update remote backup
        raw_listings_list = []
        for listing in local_raw_backup:
            raw_listings_list.append(local_raw_backup[listing])
        
        # Clear Remote raw listings backup, and update with local raw backup
        self.db.raw_listings_backup.drop()
        result = self.db.raw_listings_backup.insert_many(raw_listings_list)


    def update_formatted_listings(self):
        """
        Update formatted listings collection to match newest scrape.
        """
        print("\n********** UPDATING FORMATTED LISTINGS **********\n")
        # Get raw_listings from collection
        raw_listings_collection = get_collection_as_dict(self.db.raw_listings, raw=True)

        # Iterate through raw_listings, adding to formatted_listings.json
        for unit in raw_listings_collection:
            raw_listings_collection[unit]['_id'] = str(raw_listings_collection[unit]['_id'])
            write_to_formatted_json(raw_listings_collection[unit], self.college)

        # Copy formatted_listings into list
        formatted_listings_list = []
        if os.stat(self.formatted_listings_json).st_size != 0:
            with open(self.formatted_listings_json) as fl:
                formatted_listings_dict = json.load(fl)
        else:
            formatted_listings_dict = dict()

        for listing in formatted_listings_dict:
            formatted_listings_list.append(formatted_listings_dict[listing])
        
        # Clear Remote formatted collection, and copy formatted_listings_list to remote collection
        self.db.formatted_listings.drop()
        result = self.db.formatted_listings.insert_many(formatted_listings_list)


    def backup_formatted_listings(self):
        """
        Update local and remote archives with new remote formatted_listings collection
        """
        print("\n********** BACKING UP FORMATTED LISTINGS **********\n")

        # Get remote listings collection as list
        remote_listings = get_collection_as_dict(self.db.formatted_listings)

        # Read from local file
        if os.stat(self.formatted_backup).st_size != 0:
            with open(self.formatted_backup) as fb:
                local_formatted_backup = json.load(fb)
        else:
            local_formatted_backup = dict()

        # Update local backup
        for listing in remote_listings:
            local_formatted_backup[listing] = remote_listings[listing]
            local_formatted_backup[listing]['last_scraped'] = str(datetime.datetime.now())

        # Update remote backup
        formatted_listings_list = []
        for listing in local_formatted_backup:
            formatted_listings_list.append(local_formatted_backup[listing])
        
        # Clear Remote formatted listings backup, and update with local formatted backup
        self.db.formatted_listings_backup.drop()
        result = self.db.formatted_listings_backup.insert_many(formatted_listings_list)

        # Save formatted backup to local file
        with open(self.formatted_backup, 'w') as rb:
            json.dump(local_formatted_backup, rb, indent=4)

        

    def update_address_listings(self):
        """
        Update address_listings collection to match newest scrape.
        """
        print("\n********** UPDATING ADDRESS-BASED LISTINGS **********\n")
        # Get raw_listings from collection
        raw_listings_collection = get_collection_as_dict(self.db.raw_listings, raw=True)

        # Iterate through raw_listings, adding to address_listings.json
        for unit in raw_listings_collection:
            raw_listings_collection[unit]['_id'] = str(raw_listings_collection[unit]['_id'])
            write_to_address_json(raw_listings_collection[unit], self.college)

        # Copy address_listings into list
        address_listings_list = []
        if os.stat(self.address_listings_json).st_size != 0:
            with open(self.address_listings_json) as fl:
                address_listings_dict = json.load(fl)
        else:
            address_listings_dict = dict()

        for listing in address_listings_dict:
            address_listings_list.append(address_listings_dict[listing])
        
        # Clear Remote formatted collection, and copy formatted_listings_list to remote collection
        self.db.address_listings.drop()
        result = self.db.address_listings.insert_many(address_listings_list)


    def backup_address_listings(self):
        """
        Update local and remote archives with new remote address_listings collection
        """
        print("\n********** BACKING UP ADDRESS-BASED LISTINGS **********\n")

        # Get remote listings collection as dict
        remote_listings = get_collection_as_dict(self.db.address_listings, address=True)

        # Load local file into dict
        if os.stat(self.address_backup).st_size != 0:
            with open(self.address_backup) as fb:
                local_address_backup = json.load(fb)
        else:
            local_address_backup = dict()

        # Update local backup
        for listing in remote_listings:
            local_address_backup[listing] = remote_listings[listing]
            local_address_backup[listing]['last_scraped'] = str(datetime.datetime.now())

        # Update remote backup
        address_listings_list = []
        for listing in local_address_backup:
            address_listings_list.append(local_address_backup[listing])
        
        # Clear Remote address listings backup, and update with local address backup
        self.db.address_listings_backup.drop()
        result = self.db.address_listings_backup.insert_many(address_listings_list)

        # Save formatted backup to local file
        with open(self.address_backup, 'w') as rb:
            json.dump(local_address_backup, rb, indent=4)

    
    def summarize_scrape(self):
        """Prints summary stats for scrape"""

        print_green("Successful Scrape")
        with open("./" + self.stats_log, 'r') as sl:
            stats = json.load(sl)
            print("Total Listings:", stats["total_listings_scraped"])
            print("Unique Listings:", stats["unique_listings_scraped"])
            print("New Listings:", stats["new_listings"])
            print("Retired Listings:", stats["retired_listings"])
            print("Skipped:", stats["total_skipped"])



    
    def start(self):
        """
        Initiaties Scraping Process.
        """
        self.prepare_logs()
        self.scrape()
        self.update_location_data()
        new, retired = self.compare_raw_listings()
        self.update_raw_listings(new, retired)
        self.backup_raw_listings()
        self.update_formatted_listings()
        self.backup_formatted_listings()
        self.update_address_listings()
        self.backup_address_listings()
        self.summarize_scrape()
        

if __name__ == '__main__':
    s = Scraper('OSU', ['corvallis'], 'OR')
    s.start()
