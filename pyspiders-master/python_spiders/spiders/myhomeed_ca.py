# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class MyhomeedCaSpider(scrapy.Spider):
    name = 'myhomeed_ca'
    allowed_domains = ['myhomeed.ca']
    start_urls = ['https://www.myhomeed.ca/edmonton-rental-search/']  # https not http
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
        rentals = response.css('#properties-listing > li')
        counter = 1
        for rental in rentals:
            item_loader = ListingLoader(response=response)
            description = ' '.join(rental.css('.reset p::text').extract())
            square_meters = (re.findall('(\d*,?\d+) sq. ft.',description))
            if square_meters:
                square_meters = int(extract_number_only(extract_number_only(square_meters[0])))
            else:
                square_meters = None

            room_count = re.findall('(\d) bedroom',description)
            if room_count:
                room_count = int(room_count[0])
            else:
                room_count = None

            rent = re.findall('(\d*,?\d+) per month',description)
            if rent:
                rent = int(extract_number_only(extract_number_only(rent[0])))
            else:
                rent = None

            deposit = re.findall('(\d*,?\d+) security deposit',description)
            if deposit:
                deposit = int(extract_number_only(extract_number_only(deposit[0])))
            else:
                deposit = None

            title = rental.css('h3::text').extract_first()
            address = rental.css('.property-location p::text').extract_first()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            property_type = rental.css('.property-type::text').extract_first()
            if property_type.lower() == 'townhouse':
                property_type = 'house'
            else:
                property_type = 'apartment'

            images =rental.css('img::attr(src)').extract()
            images = [i for i in images if i[:4] =='http']


            # # MetaData
            item_loader.add_value("external_link", response.url+'#'+str(counter))  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title) # String
            # item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            # item_loader.add_value("bathroom_count", bathroom_count) # Int

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
            item_loader.add_value("external_images_count", len(images)) # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'myhomeed') # String
            item_loader.add_value("landlord_phone", '780-474-5706') # String
            item_loader.add_value("landlord_email", 'info@myhomeed.ca') # String

            self.position += 1
            counter +=1
            yield item_loader.load_item()
