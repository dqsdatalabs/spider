# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import math 

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Howoge_deSpider(Spider):
    name = 'howoge_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.howoge.de"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    

    def start_requests(self):
        pages = [1,2]
        for page in pages:            
            yield FormRequest(url=f"http://www.howoge.de/?type=999&tx_howsite_json_list[action]=immoList",
                        callback=self.parse,
                        formdata = {
                            "tx_howsite_json_list[page]":f"{page}",
                            "tx_howsite_json_list[limit]":"100",
                            "tx_howsite_json_list[rooms]":"egal",
                            "tx_howsite_json_list[wbs]":"all-offers"
                        },
                        method='POST')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["immoobjects"]:
            yield Request(response.urljoin(property_data["link"]), callback=self.populate_item, meta = {"property_data": property_data})


    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        external_id = str(property_data["uid"])
        title = property_data["title"]
        rent = str(property_data["rent"])
        rent = rent.split(".")[0]
        currency = "EUR"
        square_meters = property_data["area"]
        room_count = property_data["rooms"]
        features = " ".join(property_data["features"]).lower()

        balcony = "balkon" in features
        terrace = "terrasse" in features

        latitude = property_data["coordinates"]["lat"]
        longitude = property_data["coordinates"]["lng"]

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("figure.item a::attr(href)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        energy_label = response.css("th:contains('Energieeffizienzklasse') + td::text").get()

        landlord_name = "howoge"
        landlord_email = "vermietung@howoge.de"

        features = response.css("div.features div.feature::text").getall()
        features = " ".join(features)

        elevator = "Aufzug" in features
        utilities = response.css("p:contains('Nebenkosten') + div::text").get()
        utilities = utilities.split(",")[0]

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        # item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        # item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
