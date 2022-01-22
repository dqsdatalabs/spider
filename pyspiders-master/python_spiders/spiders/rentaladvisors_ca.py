# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import math

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Rentaladvisors_caSpider(Spider):
    name = 'rentaladvisors_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.rentaladvisors.ca"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()
    }
    
    def start_requests(self):
        yield Request(url=f"https://api.theliftsystem.com/v2/search?locale=en&client_id=284&auth_token=sswpREkUtyeYjeoahA2i&city_id=408&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2500&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=100&neighbourhood=&amenities=&promotions=&city_ids=202%2C298%2C408%2C411%2C750%2C844%2C845%2C983%2C1554%2C1879%2C2619%2C2695%2C2860%2C3010%2C3044&pet_friendly=&offset=0&count=false",
                    callback=self.parse,
                    method='GET')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            yield Request(property_data["permalink"], callback=self.populate_item, meta = {"property_data": property_data})

    def populate_item(self, response):
        
        property_data = response.meta.get("property_data")

        title = property_data["building_header"]
        rent = str(math.ceil(float(property_data["statistics"]["suites"]["rates"]["average"])))
        currency = "CAD"

        external_id = str(property_data["id"])

        room_count = str(math.ceil(float(property_data["statistics"]["suites"]["bedrooms"]["average"])))
        bathroom_count = str(math.ceil(float(property_data["statistics"]["suites"]["bathrooms"]["average"])))
        square_meters = str(math.ceil(float(property_data["statistics"]["suites"]["square_feet"]["average"])))
        available_date = property_data["availability_status_label"]
        
        property_type = property_data["property_type"]
        if("house" in property_type.lower()):
            property_type = "house"
        else: 
            property_type = "apartment"
        
        address = property_data["address"]["address"]
        city = property_data["address"]["city"]
        zipcode = property_data["address"]["postal_code"]
        latitude = str(property_data["geocode"]["latitude"])
        longitude = str(property_data["geocode"]["longitude"])

        undesired_section = r"\*\*Please note: Additional Condominium Corporation Move-in/out fees may apply\*\* This property is professionally managed by For more rental properties, please visit our website or copy the below link and paste it into a new tab /"
        description = response.css("div.main p::text").getall()
        description = " ".join(description)
        description = re.sub(r"\s+", " ", description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(undesired_section, '', description, flags=re.MULTILINE)
        description = re.sub("https:\/\/www.rentaladvisors.ca", '', description, flags=re.MULTILINE)

        images = response.css("div.slickslider a[rel='property']::attr(href)").getall()

        amenities = response.css("div.amenity-holder::text").getall()
        amenities = " ".join(amenities)

        parking = "parking" in amenities
        dishwasher = "Dishwasher" in amenities
        washing_machine = "Washer" in amenities
        pets_allowed = "Pet friendly" in amenities
        balcony = "Balcony" in amenities

        landlord_name = "rentaladvisors"
        landlord_phone = property_data["client"]["phone"]
        landlord_email = property_data["client"]["email"]

        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        # # item_loader.add_value("deposit", deposit) # Int
        # # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # # #item_loader.add_value("water_cost", water_cost) # Int
        # # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
