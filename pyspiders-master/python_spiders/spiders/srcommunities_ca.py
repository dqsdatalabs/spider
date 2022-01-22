# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math
import json

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Srcommunities_caSpider(Spider):
    name = 'srcommunities_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.srcommunities.ca"]
    start_urls = ["https://www.srcommunities.ca/"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()
    }

    def parse(self, response):
        for url in response.css("a:contains('Apartments')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.get_city_page)

    def get_city_page(self, response):
        city_script_id = response.css("script:contains('city.id ')::text").get()
        city_id = re.findall("city.id = ([0-9]+);", city_script_id)[0]
        api = f"https://api.theliftsystem.com/v2/search?show_custom_fields=true&client_id=100&auth_token=sswpREkUtyeYjeoahA2i&city_id={city_id}&geocode=&min_bed=-1&max_bed=3&min_bath=1&max_bath=3&min_rate=0&max_rate=2500&region=&keyword=false&property_types=apartments%2C+houses&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=15&offset=0&count=false" 
        yield Request (
            url = api,
            callback = self.get_properties,
            method = "GET",
            dont_filter = True
        )

    def get_properties(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            yield Request(
                url = property_data['permalink'],
                callback = self.populate_item,
                meta = {"property_data": property_data}
            )

    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        title = response.css("h1.property-title::text").get()
        description = response.css("div.cms-content p::text").getall()
        description = " ".join(description)
        images = response.css("ul.slides").css("img::attr(src)").getall()

        currency = "CAD"
        external_id = str(property_data["id"])
        address = property_data["address"]["address"]
        city = property_data["address"]["city"]
        zipcode = property_data["address"]["postal_code"]

        latitude = str(property_data["geocode"]["latitude"])
        longitude = str(property_data["geocode"]["longitude"])

        landlord_name = property_data["client"]["name"]
        landlord_phone = property_data["client"]["phone"]
        landlord_email = property_data["client"]["email"]

        amenities = response.css("section.amenities").css("li::text").getall()
        amenities = " ".join(amenities)

        balcony = "Balcon" in amenities
        parking = "parking" in amenities
        washing_machine = "Laundry" in amenities
        elevator = "Elevator" in amenities
        dishwasher = "Dishwasher" in amenities
        swimming_pool = "pool" in amenities

        suites = response.css("div.suite")
        property_id = 1
        for suite in suites:
            rent = suite.css("span.rate::text").get()
            room_count = suite.css("div.number::text").get()
            if(not re.search("([1-9])", room_count)):
                room_count = "1"
            bathroom_count = suite.css("span.bath::text").get()
            bathroom_count = math.ceil(float(bathroom_count))
            available_date = suite.css("div.available span::text").get()


            item_loader = ListingLoader(response=response)
        #     # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_id}") # String
            property_id += 1
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            self.position += 1
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # # Monetary Status
            item_loader.add_value("rent_string", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", currency) # String

            # # #item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            yield item_loader.load_item()
