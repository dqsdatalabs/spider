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


class RlgCaSpider(scrapy.Spider):
    name = "rlg_ca"
    start_urls = ['https://www.rlg.ca/']
    allowed_domains = ["rlg.ca"]
    country = 'Canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(
            url='https://connect.propertyware.com/api/marketing/listings?website_id=247627776&widget_id=9338&include_for_rent=true&include_for_sale=false',
            callback=self.parse,
            headers={
                "Authorization": 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJicmVudEBwYXRyb2NoLmNvbSIsImF1ZCI6ImFwaS5wcm9wZXJ0eXdhcmUuY29tIiwiZmlyc3ROYW1lIjoiQnJlbnQiLCJsYXN0TmFtZSI6IlBhdHJvY2giLCJpc3MiOiJpZGVudGl0eS5wcm9wZXJ0eXdhcmUuY29tIiwiZXhwIjoxNjM5OTI5NzgwLCJ1c2VySWQiOjE3NDg0Njc3MTIsImlhdCI6MTYzOTkyMjU4MCwib3JnSWQiOjI0NzcyNjA4MCwianRpIjoiMzNhMjEyMWEtY2UwMS00MGYxLTkyNGItOTIyYTliNGZjMjAxIn0.yFBsOEQhQ_1znsZf_Wmn98lJTM7PIgb7FjMN2ysFeUAntg2pmD6Z351KROmwq9-_GMfiCF2ltPEXwINCHZ5t7Q'},
            method='GET')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = str(item['id'])
            url = "https://rlg.ca/rentals/available-properties/property/" + url
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta["item"]
        external_id = item["name"]
        address = item["address"]
        zipcode = item["zip"]
        latitude = str(item["lattitude"])
        longitude = str(item["longitude"])
        city = item["city"]
        room_count = int(item["no_bedrooms"])
        bathroom_count = str(item["no_bathrooms"])
        if '.' in bathroom_count:
            bathroom_count = int(bathroom_count[0]) + 1
        else:
            bathroom_count = int(bathroom_count[0])
        rent = str(item["target_rent"])
        rent = int(rent[:-2])
        deposit = str(item["target_deposit"])
        deposit = deposit[:-2]
        deposit = int(''.join(x for x in deposit if x.isdigit()))
        square_meters = str(item["total_area"])
        square_meters = int(square_meters[:-2])
        available_date = item["available_date"]
        available_date = dateparser.parse(available_date)
        available_date = available_date.strftime("%Y-%m-%d")
        title = item["posting_title"]
        description = item["description"]
        landlord_name = item["propertyManagers"][0]["name"]
        landlord_phone = item["propertyManagers"][0]["work_phone"]
        landlord_email = item["propertyManagers"][0]["email"]
        pets_allowed = item["pets_allowed"]
        property_type = item["type"]
        if 'house' in property_type.lower():
            property_type = 'house'
        elif 'condo' in property_type.lower():
            property_type = 'apartment'

        total_images = item["images"]
        images = []
        for i in total_images:
            image = i["original_image_url"]
            images.append(image)

        total_amenities = item["amenities"]
        amenities = []
        for i in total_amenities:
            amenity = i["name"]
            amenities.append(amenity)
        amenities = ' '.join(amenities)
        washing_machine = None
        if 'washer' in amenities.lower():
            washing_machine = True
        parking = None
        if 'parking' in amenities.lower():
            parking = True
        balcony = None
        if 'balcony' in amenities.lower():
            balcony = True
        swimming_pool = None
        if 'pool' in amenities.lower():
            swimming_pool = True
        dishwasher = None
        if 'dishwasher' in amenities.lower():
            dishwasher = True
        elevator = None
        if 'elevator' in amenities.lower():
            elevator = True


        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
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
