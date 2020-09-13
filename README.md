# Corvallis Off Campus Listings Data Scraper

Collection of web scrapers, built to feed REST API for [Corvallis Off Campus Mobile App](https://github.com/FellowshipOfThePing/Corvallis-Off-Campus-Mobile)
<br/>

• Built using [Selenium](https://github.com/SeleniumHQ/selenium) and [BeautifulSoup4](https://pypi.org/project/beautifulsoup4/)

• Collects 500+ unique listings from ~15 different sites daily, feeds into set of MongoDB collections

## Core Files ##

• Core scraping structure - [Scraper.py](https://github.com/FellowshipOfThePing/Corvallis-Off-Campus-Scraper/blob/master/Data/scraping/Scraper.py)

• Additional API calls used to supplement location data - [add_features.py](https://github.com/FellowshipOfThePing/Corvallis-Off-Campus-Scraper/blob/master/Data/scraping/add_features.py)

• Error logging, duplicate listing detection, file formatting, and post-scrape reporting - [logger.py](https://github.com/FellowshipOfThePing/Corvallis-Off-Campus-Scraper/blob/master/Data/scraping/add_features.py)