# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json

from scrapy import Spider, Request, FormRequest
from scrapy.http import HtmlResponse
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Killamreit_comSpider(Spider):
    name = 'killamreit_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.killamreit.com"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    

    def start_requests(self):
        pages = [1,2]
        for page in pages:            
            yield Request(url=f"https://killamreit.com/property-geojson-feed",
                        callback=self.parse,
                        method='GET')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["features"]:
            url = re.findall(f"<a href=\"(.+)\"", property_data["properties"]["name"])[0]
            yield Request(response.urljoin(url), callback=self.populate_item, meta = {"property_data": property_data["properties"]}, dont_filter = True)


    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        title = property_data["title_1"]
        city = property_data["field_prop_city"]
        provenice = property_data["field_prop_province"]
        address = property_data["field_address"]

        address = f"{address}, {city}, {provenice}"
        try:
            location_data = extract_location_from_address(address)
            latitude = str(location_data[1])
            longitude = str(location_data[0])
        except:
            latitude = None
            longitude = None

        try:
            features = property_data["field_features"].lower()
        except:
            features = ""
        try:
            amenities = property_data["field_amenities"].lower()
        except:
            amenities = ""
        
        all_features = f"{features}, {amenities}"

        balcony = "balcony" in all_features
        dishwasher = "dishwasher" in all_features
        parking = "parking" in all_features
        washing_machine = "dryer" in all_features or "laundry" in all_features
        elevator = "elevator" in all_features
        pets_allowed = "cat friendly" in all_features or "dog friendly" in all_features

        external_id = str(property_data["nid"])
        images = response.css("div.c-gallery div.item-list ul li img::attr(src)").getall()

        description = response.css("div.view-content div.u-margin-vertical-large p::text").getall()
        description = " ".join(description)
        landlord_name = response.css("div#property-details-contact-info").css("p.c-property__contact-email::text").get()
        landlord_phone = response.css("div#property-details-contact-info").css("p.c-property__contact-phone strong a::text").get()
        landlord_email = response.css("p.c-property__contact-email strong a::attr(href)").get()
        landlord_email = landlord_email.split(":")[1]
        unites_for_rent = response.css("div.content table.cols-4 tbody tr")
        currency = "CAD"

        proeprty_status = response.css("h4.c-property__contact-title::text").get()
        if( proeprty_status == "Currently there is no availability at this location. Please contact us for more information."):
            return

        unit_id = 1
        for unit in unites_for_rent:
            unit_data = unit.css("td")
            if(len(unit_data) > 0):
                rent = unit_data[0].css("td::text").getall()
                rent = "".join(rent).strip()
                if(not re.search("([0-9]+)", rent)):
                    continue
                rent = rent.split(".")[0]
                rent = re.findall("([0-9]+)", rent)
                rent = "".join(rent)
                room_count = unit_data[1].css("td::text").getall()
                room_count = "".join(room_count).strip()
                if(not re.search("([0-9]+)", room_count)):
                    room_count = "1"
                try: 
                    available_date = unit_data[2].css("td span::text").get()
                except:
                    available_date = None

            item_loader = ListingLoader(response=response)
            # # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{unit_id}") # String
            unit_id += 1
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            self.position += 1
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            # item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            # # # item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

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
