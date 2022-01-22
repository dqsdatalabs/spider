# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import urllib
import math

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Bwalk_comSpider(Spider):
    name = 'bwalk_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.bwalk.com"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    
    headers = {
        "Accept": "application/json, text/plain, */*, application/json",
        "Accept-Language": "en-CA",
        "Accept-Encoding": "gzip, deflate, br"
    }

    def start_requests(self):
        yield FormRequest(url=f"https://www.bwalk.com/umbraco/api/availabilityproxy/locations?brandId=",
                    callback=self.parse,
                    headers = self.headers,
                    method='GET')

    def parse(self, response):
        url = "https://www.bwalk.com/umbraco/api/availabilityproxy/projects?cityId="
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["data"]:
            yield Request(f"{url}{property_data['city']['id']}", 
            callback=self.get_properties, 
            headers = self.headers,
            meta = {"city_data": property_data["city"]})

    def get_properties(self, response):
        city_data = response.meta.get("city_data")
        url = "https://www.bwalk.com/en-ca/rent/details"
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["data"]:
            province = property_data["province"]["name"]
            city = property_data["city"]["name"]
            name = property_data["name"]
            properties_url = f"{url}/" + urllib.parse.quote(f"{province}/{city}/{name}")
            yield Request(url = properties_url, 
            callback=self.get_items,
            meta = {"property_data": property_data})

    def get_items(self, response):
        property_data = response.meta.get("property_data")
        for item_url in response.css("div.offer-text a::attr(href)").getall():
            yield Request(response.urljoin(item_url), 
            callback=self.populate_item,
            meta = {"property_data": property_data})


    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        title = response.css("h1.offer-card-title::text").get()
        rent_values = response.css("p.offer-card-price::text").getall()
        rent_values = " ".join(rent_values)
        rent_values = re.findall("([0-9]+)", rent_values)

        currency = "CAD"
        external_id = str(property_data["id"])
        room_count = re.findall("([0-9]) Bedroom", title)
        if(len(room_count) == 0):
            room_count = "1"
        else:
            room_count = room_count[0]
    
        bathroom_count = response.css("li:contains('Bathrooms:') span.feature-value::text").get()
        if("-" in bathroom_count):
            bathroom_count = bathroom_count.split("-")[1] 
        bathroom_count = math.ceil(float(bathroom_count))
        square_meters = response.css("li:contains('Sq Ft:') span.feature-value::text").get()
        if("-" in square_meters):
            square_meters = square_meters.split("-")[0]
        
        deposit = response.css("li:contains('Deposit:') span.feature-value::text").get()

        features = response.css("div.container").css("div.feature-icon span.feature-text::text").getall()
        features = " ".join(features)
        dishwasher = "Dishwasher" in features
        balcony = "Balcony" in features
        washing_machine = "Laundry" in features
        pets_allowed = "Allows Cats" in features or "Allows Dogs" in features
        parking = "Parking" in features
        elevator = "Elevator" in features
        images = response.css("picture img::attr(srcset)").getall()
        images = [image_src.split(" ")[0] for image_src in images]  

        latitude = str(property_data["geolocation"]["latitude"])
        longitude = str(property_data["geolocation"]["longitude"])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        landlord_name = "bwalk"
        landlord_phone = property_data["phoneNumber"]
        landlord_email = property_data["email"]
        
        property_id = 0
        for rent in rent_values:

            item_loader = ListingLoader(response=response)
            # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_id}") # String
            property_id += 1

            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            self.position += 1
            item_loader.add_value("title", title) # String
            # # item_loader.add_value("description", description) # String

            # # city Details
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

            # # # item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
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
            item_loader.add_value("deposit", deposit) # Int
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
