# -*- coding: utf-8 -*-
# Author: Madhumitha S
# Team: Sabertooth

from time import strptime
import scrapy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, extract_number_only, format_date
from ..user_agents import random_user_agent
from geopy.geocoders import Nominatim
import re
import lxml.etree
import js2xml
from scrapy import Selector
from datetime import datetime
from datetime import date
import requests
from math import ceil
import dateparser

class WilliamsLynchSpider(scrapy.Spider):
    name = 'williamslynch_co_uk'
    allowed_domains = ['williamslynch.co.uk']
    start_urls = ['https://williamslynch.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = ['https://williamslynch.co.uk/properties-rss.xml']
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        property_urls = response.xpath(".//channel//link/text()").extract()[1:]
        for url in property_urls:
            yield scrapy.Request(url=url, callback=self.parse_properties,
                                 meta={'request_url': url},
                                 dont_filter=True)

    def parse_properties(self, response):
        item_loader = ListingLoader(response=response)
        price = response.xpath(".//h3[@class='MuiTypography-root jss19 MuiTypography-h3']/text()").extract_first()

        # To get only the rental properties
        if "pcm" not in price and "pw" not in price:
            return

        external_id = response.meta.get('request_url').split("/")[-2]
        external_link = "https://williamslynch.co.uk/properties/" + external_id
        # title = response.xpath(".//h2[@class='MuiTypography-root jss19 MuiTypography-h2']/text()").extract_first()

        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', response.meta.get('request_url').split("/")[-2])
        item_loader.add_xpath('title', ".//article/section//h2/text()")

        if price and len(re.findall(r"[^a-z]+pcm", price)) > 0:
            item_loader.add_value('rent_string', re.findall(r"[^a-z]+pcm", price)[0])
        elif price and len(re.findall(r"[^a-z]+pw", price)) > 0:
            price = re.findall(r"[^a-z]+pw", price)[0]
            price = extract_rent_currency(price, WilliamsLynchSpider)[0]
            item_loader.add_value('rent_string', "£ " + str(price))

        address = response.xpath(".//title[@data-react-helmet='true']/text()").extract_first().split(" | ")[0]
        if address:
            item_loader.add_value('address', address)
            zipcode = address.split(",")[-1].strip()
            if zipcode.replace(" ","").isalpha():
                item_loader.add_value('city', zipcode)
            else:
                item_loader.add_value('zipcode', zipcode)
                item_loader.add_value('city', address.split(",")[-2].strip())

        room_count = response.xpath(".//span[contains(text(),'Bedrooms')]/preceding-sibling::div/text()").extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)

        bathroom_count = response.xpath(".//span[contains(text(),'Bathrooms')]/preceding-sibling::div/text()").extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)

        item_loader.add_xpath('images', ".//picture//@src")
        floor_plan_images = response.xpath(".//span[contains(text(),'Floor Plan')]/ancestor::a/@href").extract_first()
        if floor_plan_images and "undefined" not in floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        description = response.xpath(".//article/section/div/p/text()").extract()[0:-1]
        if "exclude" in description[0].lower() or "include" in description[0].lower():
            item_loader.add_value('description', remove_unicode_char(" ".join(description[1:])))
        else:
            item_loader.add_value('description', remove_unicode_char(" ".join(description)))
        
        description = " ".join(response.xpath(".//article/section/div/p/text()").extract()[0:-1])
        square_feet = re.search('(\d+) sq ft', description, re.IGNORECASE)
        if square_feet:
            square_feet = square_feet.group(1)
            square_meters = str(int(ceil(float(square_feet) * 0.093)))
            item_loader.add_value('square_meters', square_meters)
        
        features = " ".join(response.xpath(".//article/section/div/li/text()").extract())

        availability = description + features
        if "available to" in availability.lower():
            item_loader.add_value('available_date', datetime.today().strftime('%Y-%m-%d'))
        elif "available now" in availability.lower():
            item_loader.add_value('available_date', datetime.today().strftime('%Y-%m-%d'))
        elif "available from" in availability.lower():
            check = re.search('Available from (\d{2}\w{2} \w+)', availability, re.IGNORECASE)
            if check:
                available_date = check.group(1)
                available_date = available_date.lower().replace("rd", "").replace("nd", "").replace("st", "").replace("th","") + " " + str(datetime.now().year)
                if available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                    if date_parsed:
                        item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'holiday complex']
        studio_types = ["studio"]
        #Hard coding
        unknown_type = ["development"]
        if any(i in availability.lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in availability.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in availability.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        elif any(i in availability.lower() for i in unknown_type):
            item_loader.add_value('property_type', 'apartment')
        else:
            item_loader.add_value('property_type', 'apartment')
        
        # 'liarking' instead of 'parking' in some properties: ex: https://williamslynch.co.uk/properties/2908716
        if "parking" in features.lower() or "liarking" in features.lower():
            item_loader.add_value('parking', True)

        # https://williamslynch.co.uk/properties/4740889
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "swimming" in features.lower() or "pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        # https://williamslynch.co.uk/properties/4801676
        if "furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)
        
        # https://williamslynch.co.uk/properties/4344219
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        """
        geolocator = Nominatim(user_agent=random_user_agent())
        location = geolocator.geocode(item_loader.get_output_value('address'),addressdetails=True)
        if location:
            item_loader.add_value('latitude', str(location.latitude))
            item_loader.add_value('longitude', str(location.longitude))
            if 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
        """

        item_loader.add_value('landlord_phone', '020 7940 9940')
        item_loader.add_value('landlord_email', 'info@williamslynch.co.uk')
        item_loader.add_value('landlord_name', 'Williams Lynch')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Williamslynch_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()
