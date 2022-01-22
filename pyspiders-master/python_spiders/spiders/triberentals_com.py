# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json 
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Triberentals_comSpider(Spider):
    name = 'triberentals_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.triberentals.com"]
    start_urls = ["https://www.triberentals.com/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.menu-main li.city a::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages)

    def get_pages(self, response):
        for city_id in response.css("div::attr(data-city-id)").getall():
            url = f"https://api.theliftsystem.com/v2/search?show_promotions=true&client_id=118&auth_token=sswpREkUtyeYjeoahA2i&city_id={city_id}&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=3100&region=&keyword=false&property_types=&order=&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false"
            yield Request(response.urljoin(url), callback=self.pars_properties, dont_filter = True)
            
    def pars_properties(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            yield Request(property_data["permalink"], callback=self.populate_item, meta = {"property_data": property_data})
        
    def populate_item(self, response):
        
        property_type = "apartment"

        property_data = response.meta.get("property_data")
        
        latitude = str(property_data["geocode"]["latitude"])
        longitude = str(property_data["geocode"]["longitude"])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        title = response.css("h1.title span::text").get()
        
        currency = "CAD"
        images = response.css("div.cover::attr(style)").getall()
        images = [image_src.split("background-image:url('")[1].split("');")[0] for image_src in images]
        description = response.css("div.cms-content p::text").getall()
        description = " ".join(description)

        amenities = response.css("div.cms-content").css("li::text").getall()
        amenities = " ".join(amenities)
        elevator = "Elevator" in amenities
        dishwasher = "Dishwasher" in amenities
        washing_machine = "Laundry" in amenities
        
        pets_allowed = property_data["pet_friendly"]
        
        if( pets_allowed == "n/a"):
            pets_allowed = None
        
        landlord_name = "triberentals"
        landlord_phone = response.css("span.phone-number::text").get()

        suites = response.css("div.suite-table-body")

        property_id = 1
        for suite in suites:
            
            rent = suite.css("div.table-body-rate::text").get()
            if(not re.search("([0-9]+)", rent)):
                continue
            
            room_count = property_data["statistics"]["suites"]["bedrooms"]["max"]
            if( not re.search("([1-9])", str(room_count))):
                room_count = "1"
            bathroom_count = suite.css("div.table-body-baths::text").get()
            bathroom_count = math.ceil(float(bathroom_count))

            external_id = suite.css("div.table-body-number::text").get()

            square_meters = suite.css("div.table-body-sqft::text").get()
            available_date = suite.css("div.available a::text").get()

            item_loader = ListingLoader(response=response)
            # # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_id}") # String
            property_id += 1
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
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # item_loader.add_value("furnished", furnished) # Boolean
            # # item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            # item_loader.add_value("balcony", balcony) # Boolean
            # # #item_loader.add_value("terrace", terrace) # Boolean
            # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # # Monetary Status
            item_loader.add_value("rent_string", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", currency) # String

            # # #item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # # #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            yield item_loader.load_item()
