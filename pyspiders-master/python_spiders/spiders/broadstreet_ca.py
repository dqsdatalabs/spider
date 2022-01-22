# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Howoge_deSpider(Spider):
    name = 'broadstreet_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.broadstreet.ca"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }

    headers = {
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5"
    }

    numbers_dict = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "2.5": 3,
        "1.5": 2
    }

    def start_requests(self):
        yield Request(url=f"https://website-gateway-cdn.rentsync.com/v1/broadstreet/search?propertyFilters=categoryTypes:residential&unitFilters=availability:available&order=property:buildingName~asc&include=geoLocation,photos,geoLocation&groupUnitSummaryBy=availability&limit=50&addSelect=phone,buildingOverview",
                    callback=self.parse,
                    headers = self.headers,
                    method='GET')

    def parse(self, response):
        url = "https://www.broadstreet.ca/residential/"
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["data"]:
            yield Request(f'{url}{property_data["permaLink"]}', callback=self.populate_item, meta = {"property_data": property_data})

    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        external_id = str(property_data["id"])
        title = response.css("h1.property__title::text").get()
        address = response.css("p.property__address::text").get()

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("div.hero-section__slider-image::attr(style)").getall()
        images = [image_src.split('background-image: url("')[1].split('");')[0] for image_src in images]

        description = response.css("div.property__desc-content p::text").getall()
        description = " ".join(description)

        currency = "CAD"

        amenities = response.css("li.amenity p.text::text").getall()
        amenities = " ".join(amenities)

        dishwasher = "Dishwasher" in amenities
        washing_machine = "Washer/Dryer" in amenities
        balcony = "Balcony" in amenities
        parking = "Parking" in amenities
        elevator = "Elevator" in amenities
        pets_allowed = "Pet Friendly" in amenities

        deposit = response.css("p:contains('deposit')::text").get()
        deposit = re.findall("([0-9]+)", deposit)
        deposit = ''.join(deposit)

        utilities = response.css("p:contains('month-to-month fee')::text").get()
        if(utilities):
            utilities = re.findall("([0-9]+)", utilities)
            utilities = ''.join(utilities)

        landlord_name = "broadstreet"
        landlord_phone = response.css("div.property__phone-flex a::text").get()

        suites = response.css("div.suite")

        property_id = 0
        for suite in suites:
            rent = suite.css("span.suite__rate--text span::text").get()
            if(not rent ):
                continue
            if( not re.search("([0-9]+)", rent)):
                continue

            square_meters = suite.css("span.suite__sqft--text::text").get()
            available_date = suite.css("span.suite__availability-text--text::text").get()

            suite_title = suite.css("span.suite__name--text::text").get()
            room_count = re.findall("([A-Z][a-z]+|[1-9]\.[1-9]) Bedroom", suite_title)[0].lower()
            room_count = self.numbers_dict[room_count]
            
            bathroom_count = re.findall("([A-Z][a-z]+|[1-9]\.[1-9]) Bath", suite_title)[0].lower()
            bathroom_count = self.numbers_dict[bathroom_count]
            
            item_loader = ListingLoader(response=response)
            # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_id}") # String
            property_id += 1
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            # self.position += 1
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
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            # # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # # # item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # # # Monetary Status
            item_loader.add_value("rent_string", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", currency) # String

            # # # #item_loader.add_value("water_cost", water_cost) # Int
            # # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            yield item_loader.load_item()
