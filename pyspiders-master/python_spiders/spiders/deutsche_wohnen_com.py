# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import json
import re

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent


class Deutsche_wohnen_comSpider(Spider):
    name = 'deutsche_wohnen_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.deutsche-wohnen.com"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()
    }
    image_url_prefix = "https://immo-api.deutsche-wohnen.com"

    def start_requests(self):
        yield Request(url='https://immo-api.deutsche-wohnen.com/estate/findByFilter',
                      callback=self.parse,
                      body = '{"infrastructure":{},"flatTypes":{},"other":{},"commercializationType":"rent","utilizationType":"flat","location":"ber","locale":"en","radius":"5000"}',
                      headers = {"Content-Type": "application/json"},   
                      method = "POST"
                        )

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        property_page_prefix = "expose/object/"
        for property_data in parsed_response_properties:
            yield Request(f'https://{self.allowed_domains[0]}/{property_page_prefix}{property_data["id"]}', callback=self.populate_item, meta = {"property_data": property_data})

    def populate_item(self, response):
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        title = property_data["title"]
        rent = str(property_data["price"])
        if("." in rent):
            rent = rent.split(".")[0]
        currency = "EUR"
        external_id = property_data["id"]
        latitude = str(property_data["geoLocation"]["latitude"])
        longitude = str(property_data["geoLocation"]["longitude"])
        city = property_data["address"]["city"]
        zipcode = property_data["address"]["zip"]
        address = f'{property_data["address"]["street"]} {property_data["address"]["houseNumber"]} {city} {zipcode}'
        images = [self.image_url_prefix + image_src["filePath"] for image_src in property_data["images"]]
        square_meters = str(property_data["area"])
        if("." in square_meters):
            square_meters = square_meters.split(".")[0]
        room_count = int(float(property_data["rooms"]))
        
        try:
            floor = str(property_data["level"])
        except:
            floor = None

        deposit = response.css("td:contains('Kaution') + td::text").get()
        if("," in deposit):
            deposit = deposit.split(",")[0]
        deposit = re.findall("([0-9]+)", deposit)
        deposit = "".join(deposit)

        heating_cost = response.css("td:contains('Heizkosten') + td::text").get()
        if("," in heating_cost):
            heating_cost = heating_cost.split(",")[0]
        heating_cost = re.findall("([0-9]+)", heating_cost)
        heating_cost = "".join(heating_cost)
        
        description = response.css("div.object-detail__location p::text").get()

        amenities = response.css("li.object-detail__equipment-icon span.object-detail__equipment-description::text").getall()
        amenities = " ".join(amenities)

        balcony = "Balkon" in amenities
        elevator = "Aufzug" in amenities

        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        if(',' in utilities):
            utilities = utilities.split(",")[0]

        item_loader = ListingLoader(response=response)
        # # MetaData
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # #item_loader.add_value("terrace", terrace) # Boolean
        # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        # item_loader.add_value("landlord_name", landlord_name) # String
        # # item_loader.add_value("landlord_phone", landlord_phone) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
