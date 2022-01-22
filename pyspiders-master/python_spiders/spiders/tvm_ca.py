# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import *
import requests
import json
import datetime
import dateparser

class TvmCaSpider(scrapy.Spider):
    name = "tvm_ca"
    start_urls = ['https://www.tvm.ca/properties/tvm-185-hunter-street-east-inc']
    allowed_domains = ["tvm.ca"]
    country = 'Canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(
            url='https://api.theliftsystem.com/v2/search?client_id=359&auth_token=sswpREkUtyeYjeoahA2i&city_id=32917&geocode=&min_bed=-1&max_bed=10&min_bath=-1&max_bath=10&min_rate=200&max_rate=16900&min_sqft=0&max_sqft=10511&region=&keyword=false&property_types=low-rise-apartment,house,mid-rise-apartment,high-rise-apartment,luxury-apartment,multi-unit-house,single-family-home,townhouse,semi,duplex,triplex,fourplex,mobile-home,rooms&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+ASC,+min_rate+ASC,+min_bed+ASC,+max_bath+ASC&limit=200&neighbourhood=&amenities=&promotions=&city_ids=2156,32917,605,2251,3133&pet_friendly=&offset=0&count=false',
            callback=self.parse,
            body='',
            method='GET')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item = response.meta["item"]

        is_available = item['availability_status']
        if is_available:
            available_date = item['min_availability_date']
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        else:
            return

        title = item['name']
        pets_allowed = item['pet_friendly']
        if pets_allowed != True:
            pets_allowed = None
        description = item['details']['overview']
        property_type = 'apartment'
        address = item['address']['address']
        city = item['address']['city']
        zipcode = item['address']['postal_code']
        longitude = item['geocode']['longitude']
        latitude = item['geocode']['latitude']
        landlord_name = item['contact']['name']
        landlord_phone = item['contact']['phone']
        landlord_email = item['contact']['email']

        images = response.css('.cover::attr(data-src2x)').extract()
        amenities = ''
        try:
            amenities = response.css('.amenity-holder ::text').extract()
            amenities = ''.join(amenities)
        except:
            pass
        elevator = None
        if 'elevator' in amenities.lower():
            elevator = True
        dishwasher = None
        if 'dishwasher' in amenities.lower():
            dishwasher = True
        parking = None
        if 'parking' in amenities.lower():
            parking = True
        washing_machine = None
        if 'washer' in amenities.lower() or 'laundry' in amenities.lower():
            washing_machine = True
        balcony = None
        if 'balcony' in amenities.lower():
            balcony = True

        suites = response.css('.suite-type ::text').extract()
        has = response.css('.label ::text').extract()
        has = ''.join(has)
        if 'square' in has.lower():
            has_sqft = True
        else:
            has_sqft = False
        if 'suite photos' in has.lower():
            has_photos = True
        else:
            has_photos = False

        for i, x in enumerate(suites):
            item_loader = ListingLoader(response=response)
            external_link = response.url + '#' + str(i + 1)
            external_id = x
            room_count = response.css('.info-block:nth-child(1) .info::text')[i].extract()
            room_count = int(room_count)
            if room_count < 1:
                room_count += 1
            bathroom_count = response.css('.info-block:nth-child(2) .info::text')[i].extract()
            bathroom_count = int(bathroom_count)
            if bathroom_count < 1:
                bathroom_count += 1
            if has_sqft:
                square_meters = response.css('.info-block:nth-child(3) .info::text')[i].extract().strip()
                square_meters = int(''.join(x for x in square_meters if x.isdigit()))
                rent = response.css('.info-block:nth-child(4) .info::text')[i].extract().strip()
            else:
                square_meters = None
                rent = response.css('.info-block:nth-child(3) .info::text')[i].extract().strip()
            rent = int(''.join(x for x in rent if x.isdigit()))
            floor_plan_images = None
            if has_photos:
                if has_sqft:
                    floor_plan_images = [response.css('.info-block:nth-child(6) a::attr(href)')[i].extract()]
                else:
                    floor_plan_images = [response.css('.info-block:nth-child(5) a::attr(href)')[i].extract()]

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
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
