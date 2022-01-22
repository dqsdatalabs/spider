# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class KarmapropertiesCaSpider(scrapy.Spider):
    name = 'karmaproperties_ca'
    allowed_domains = ['karmaproperties.ca']
    start_urls = ['https://karmaproperties.ca/under-development']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.field-link > a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://karmaproperties.ca'+rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        the_only_text_box = (response.css('#block-views-properties-block-1 .active::text').extract_first()).lower()
        rent = re.findall('\$(\d+)',the_only_text_box)
        if rent != []:
            item_loader = ListingLoader(response=response)
            rent = rent[0]
            room_count = re.findall('(\d?.?\d) bath',the_only_text_box)
            if room_count != []:
                room_count = int(ceil(float(room_count[0])))
            else:
                room_count = None
            bathroom_count = re.findall('(\d?.?\d) bath', the_only_text_box)
            if bathroom_count != []:
                bathroom_count = int(ceil(float(bathroom_count[0])))
            else:
                bathroom_count = None
            address = re.findall('(\d+)[\s\w]+(highway|drive|street)',the_only_text_box)
            if address != []:
                address = address[0][0] + 'Canada'
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
                if zipcode == '':
                    zipcode = None
            else:
                address = None
                longitude = None
                latitude = None
                zipcode = None
                city = None

            floor = re.findall('\w+ floor',the_only_text_box)
            if floor !=[]:
                floor = floor[0]
            else:
                floor = None
            images =response.css('.slides a::attr(href)').extract()

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", 'apartment') # String
            item_loader.add_value("description", the_only_text_box) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", 'apartment') # String
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            # item_loader.add_value("parking", parking) # Boolean
            # item_loader.add_value("elevator", elevator) # Boolean
            # item_loader.add_value("balcony", balcony) # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # item_loader.add_value("washing_machine", washing_machine) # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            # item_loader.add_value("external_images_count", len(images)) # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'Karma Property') # String
            item_loader.add_value("landlord_phone", '204-415-5712') # String
            item_loader.add_value("landlord_email", 'karmaproperty@shaw.ca') # String

            self.position += 1
            yield item_loader.load_item()
