# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import format_date, sq_feet_to_meters, extract_number_only, format_date
from datetime import datetime
from dateutil.parser import parse
import json
import re

class MultiunitpropertymanagementSpider(scrapy.Spider):
    name = "MultiUnitPropertyManagement"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=473&auth_token=sswpREkUtyeYjeoahA2i&city_id=753&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1900&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=1845%2C2398%2C753&pet_friendly=&offset=0&count=false']
    allowed_domains = ["mupm.ca", "theliftsystem.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        listings = json.loads(response.body)
        for listing in listings:
            if listing['availability_status']:
                yield scrapy.Request(listing['permalink'], callback=self.populate_item, meta={ **listing })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        amenities = response.css("div.amenity-holder::text").getall()
        for index, unit in enumerate(response.css("script[type='application/ld+json']::text").getall()):
            row = response.css("div.suite.content")[index]
            unit = json.loads(unit)
            external_id = row.css("div.suite-info p.suite-num::text").get().strip()
            title = row.css('h3.suite-modal-title::text').get().strip()
            description = '\n'.join([x for x in data['details']['overview'].split('\n')])
            city = data['address']['city']
            zipcode = data['address']['postal_code']
            address = data['address']['address']
            latitude = data['geocode']['latitude']
            longitude = data['geocode']['longitude']
            
            room_count = 1
            bathroom_count = None
            square_meters = None
            for info in row.css("div.suite-info span::text").getall():
                if 'bed' in info.lower():
                    room_count = int(float(extract_number_only(info, thousand_separator=',', scale_separator='.')))
                if 'bath' in info.lower():
                    bathroom_count = int(float(extract_number_only(info, thousand_separator=',', scale_separator='.')))
                if 'sq' in info.lower():
                    square_meters = int(float(extract_number_only(info, thousand_separator=',', scale_separator='.')))
                    square_meters = square_meters if square_meters!=0 else None
            
            available_date = None
            for info in row.css("div.suite-desc span.suite-description p::text").getall():
                if 'available' in info.lower():
                    available_date = parse(info.lower().split('available')[1]).strftime("%Y-%m-%d")
            property_type = 'apartment' if 'house' not in data["property_type"] else 'house'
            pets_allowed = data['pet_friendly']
            
            parking = balcony = dishwasher = washing_machine = elevator = None
            for amenity in amenities:
                if amenity.lower().find('balcon'):
                    balcony = True
                if amenity.lower().find('dishwasher'):
                    dishwasher = True
                if amenity.lower().find('parking'):
                    parking = True
                if amenity.lower().find('landry') or amenity.lower().find('washer'):
                    washing_machine = True
                if amenity.lower().find('elevator'):
                    elevator = True
            images = response.css("section.slickslider_container a.gallery-image.img::attr(href)").getall()
            floor_plan_images = [row.css('a.floorplan-link::attr(href)').get()]
                
            rent = int(float(row.css("div.suite-rate span.suite-price::text").get().strip().split('$')[1]))
            deposit = int(float(row.css("div.suite-deposit span.suite-price::text").get().strip().split('$')[1]))
            landlord_name = data['client']['name']
            landlord_number = data['client']['phone']
            landlord_email = data['client']['email']
            
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url + f'#{index+1}') # String
            item_loader.add_value("external_source", self.external_source) # String
            item_loader.add_value("position", self.position) # Int

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
