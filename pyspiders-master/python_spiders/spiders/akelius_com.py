# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import math

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Akelius_comSpider(Spider):
    name = 'akelius_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.akelius.com"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    

    def start_requests(self):
        yield Request(url=f"https://rent.akelius.com/lettings/marketing/v2/CA/published-adverts.json",
                    callback=self.parse,
                    method='GET')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            property_id = property_data["id"]
            url = f"https://rent.akelius.com/lettings/marketing/v2/CA/{property_id}.json"
            yield Request(url = url, callback=self.populate_item, method = "GET", dont_filter = True)


    def populate_item(self, response):
        
        property_data = json.loads(response.body)
        url = f"https://rent.akelius.com/en/detail/canada/{property_data['id']}"

        external_id = str(property_data["id"])
        try:
            property_type = property_data["keyfacts"]["apartment-type"]
            if(property_type not in ["apartment", "house", "room", "student_apartment", "studio"]):
                property_type = "apartment"
        except:
            property_type = "apartment"
        rent = str(property_data["keyfacts"]["total-rent"])
        currency = property_data["localization"]["currency"]

        try:
            room_count = property_data["keyfacts"]["number-of-bedrooms"]
            if(not re.search("([0-9]+)", room_count)):
                room_count = "1"
        except:
            room_count = "1"

        try:
            bathroom_count = str(math.ceil(float(property_data["keyfacts"]["number-of-bathrooms"])))
            if(not re.search(r"([1-9])", bathroom_count)):
                bathroom_count = None

        except:
            bathroom_count = None
        try:
            square_meters = str(int(property_data["keyfacts"]["unit-size"]))
        except: 
            square_meters = None
        address = f"{property_data['address']['streetName']}, {property_data['address']['city']}, {property_data['address']['postalCode']}"
        city = property_data['address']['city']
        zipcode = property_data['address']['postalCode']

        latitude = str(property_data['address']['latitude'])
        longitude = str(property_data['address']['longitude'])
        title = property_data["keyfacts"]["streetname"]
        try:
            floor = str(property_data["keyfacts"]["floor"])
        except:
            floor = None

        try:
            balcony = property_data["keyfacts"]["has-balcony"]
        except:
            balcony = None
        
        try:
            dishwasher = property_data["keyfacts"]["has-dishwasher"]
        except:
            dishwasher = None
        try:
            washing_machine = property_data["keyfacts"]["has-washing-machine"]
        except:
            washing_machine = None
        try:
            elevator = property_data["keyfacts"]["has-elevator"]
        except:
            elevator = None
        try:
            swimming_pool = property_data["keyfacts"]["has-indoor-pool"] or property_data["keyfacts"]["has-outdoor-pool"]
        except:
            swimming_pool = None
        try:
            terrace = property_data["keyfacts"]["has-terrace"]
        except:
            terrace = None
        try:
            pets_allowed = property_data["keyfacts"]["pets-allowed"]
            if(pets_allowed != True or pets_allowed != False):
                pets_allowed = None
        except:
            pets_allowed = None

        images = [document["largeUrl"] for document in property_data["documents"]]

        landlord_name = "akelius_com"
        landlord_phone = property_data["contactDetails"]["phoneNumber"]

        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        # # item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # # # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # # item_loader.add_value("furnished", furnished) # Boolean
        # # # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
