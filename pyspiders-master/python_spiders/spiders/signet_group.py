# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import json
import datetime
import dateparser


class SignetGroupSpider(scrapy.Spider):
    name = "signet_group"
    start_urls = ['http://www.signetgroup.ca/']
    allowed_domains = ["signetgroup.ca"]
    country = 'Canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(
            url='https://api.theliftsystem.com/v2/search?client_id=84&auth_token=sswpREkUtyeYjeoahA2i&city_id=1992&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4700&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments,+houses&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC,+min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=3133,2015,3418,2566,3393,902,3339,1992&pet_friendly=&offset=0&count=false',
            callback=self.parse,
            body='',
            method='GET')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        parsed_response = response.json()
        for item in parsed_response:
            url = item["permalink"]
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item = response.meta["item"]
        external_id = str(item["id"])
        title = item["name"]
        description = response.css('#about p::text').extract()
        description = ' '.join(description)
        if description == '':
            description = None
        property_type = item["property_type"]
        if 'apartment' in property_type:
            property_type = 'apartment'
        elif 'house' in property_type.lower():
            property_type = 'house'

        latitude = item["geocode"]["latitude"]
        longitude = item["geocode"]["longitude"]
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        landlord_name = item["contact"]["name"]
        if landlord_name == None:
            landlord_name = 'Signet Group'
        landlord_phone = item["contact"]["phone"]
        landlord_email = item["contact"]["email"]
        if landlord_email == None:
            landlord_email = 'rentals@signetgroup.ca'

        pets_allowed = item["pet_friendly"]
        if pets_allowed != True:
            pets_allowed = None

        amenities = response.css('.amenity::text').extract()
        parkings = response.css('.parking-content ::text').extract()
        amenities = ' '.join(amenities)
        balcony = None
        if 'balcon' in amenities.lower():
            balcony = True
        dishwasher = None
        if 'dishwasher' in amenities.lower():
            dishwasher = True
        washing_machine = None
        if 'laundry' in amenities.lower():
            washing_machine = True
        parking = None
        if 'parking' in amenities.lower():
            parking = True
        elevator = None
        if 'elevator' in amenities.lower():
            elevator = True
        if parkings != []:
            parking = True


        list = response.css('.suite-availability span::text').extract()
        for i, x in enumerate(list):
            item_loader = ListingLoader(response=response)
            available_date = None
            if 'not available' in x.lower():
                continue
            elif 'available now' not in x.lower():
                available_date = x.strip()
                available_date = available_date.split('Available')[1]
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            rent = response.css('.rate-value::text')[i].extract()
            if 'call' in rent.lower():
                continue
            external_link = response.url + '#' + str(i + 1)
            rent = int(''.join(x for x in rent if x.isdigit()))

            room_count = response.css('.type-name::text')[i].extract()
            if any(char.isdigit() for char in room_count):
                room_count = int(''.join(x for x in room_count if x.isdigit()))
            else:
                room_count = 1
            square_meters = None
            try:
                square_meters = response.css('.suite-sqft span::text')[i].extract()
                if any(char.isdigit() for char in square_meters):
                    square_meters = int(''.join(x for x in square_meters if x.isdigit()))
                else:
                    square_meters = None
            except:
                pass
            images = None
            try:
                images = response.css('.suite-photos')[i].css(' a::attr(href)').extract()
            except:
                pass
            floor_plan_images = None
            try:
                floor_plan_images = response.css('.suite-floorplans')[i].css(' a::attr(href)').extract()
            except:
                pass


            # # MetaData
            item_loader.add_value("external_link", external_link) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

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
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
