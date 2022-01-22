# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Aptrentals_netSpider(Spider):
    name = 'aptrentals_net'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.aptrentals.net"]
    start_urls = ["https://www.aptrentals.net"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Apartments')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages)

    def get_pages(self, response):
        for url in response.css("a.residential-522::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.get_property_page)
        
    def get_property_page(self, response):
        for url in response.css("div.property-item::attr(data-path)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.property-title::text").get()
        address = response.css("h1.property-title + p::text").get()
        
        data_location = extract_location_from_address(address)
        latitude = str(data_location[1])
        longitude = str(data_location[0])

        data_location = extract_location_from_coordinates(longitude, latitude)
        zipcode = data_location[0]
        city = data_location[1]

        images = response.css("div.image img::attr(src)").getall()

        landlord_phone = "(604) 683-7690"

        description = response.css("div.cms-content:contains('Property Description')").get()
        description = re.findall("<.+>(.+)</?.+>", description)
        description = " ".join(description)
        description = re.sub("</[a-z]+>", "", description)
        description = re.sub("Property Description", "", description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(landlord_phone, '', description, flags=re.MULTILINE)
        description = description.split("Call for more information")[0]


        amenities = response.css("section.amenities ul div div.span12 li::text").getall()
        amenities = " ".join(amenities)

        balcony = "Balcon" in amenities
        dishwasher = "Dishwasher" in amenities
        washing_machine = "Washer and Dryer" in amenities
        elevator = "Elevator" in amenities
        
        pets_allowed = response.css("div.pet-policy ul li::text").get()
        
        if(pets_allowed):
            if("Pets Not Allowed" in pets_allowed):
                pets_allowed = False
            if(pets_allowed):
                if("Pet Friendly" in pets_allowed or "Allowed" in pets_allowed):
                    pets_allowed = True

        parking_fees = response.css("div.parking p::text").getall()
        parking_fees = " ".join(parking_fees)
        parking = "Parking" in parking_fees

        currency = "CAD"
        landlord_name = "aptrentals"
        landlord_email = "vp@aptrentals.net"

        property_record = 1
        suites  = response.css("div.suite")

        for suite in suites:
            rent = suite.css("div.suite-rate span.value::text").get()
            if(rent):
                rent = rent.strip()
            else:
                continue

            if(not re.search("([0-9]+)", rent)):
                continue
            rent = re.findall("([0-9]+)",rent)[0]
            room_count = suite.css("div.suite-type div.value::text").get()
            try:
                room_count = re.findall("([0-9])",room_count)[0]
            except: 
                room_count = "1"
            square_meters = suite.css("div.suite-sqft div.hidden-mobile span.value::text").get()
            if(square_meters):
                square_meters = re.findall("([0-9]+)",square_meters)[0]

            item_loader = ListingLoader(response=response)
            # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_record}") # String
            property_record += 1
            item_loader.add_value("external_source", self.external_source) # String

            # item_loader.add_value("external_id", external_id) # String
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
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            # # item_loader.add_value("bathroom_count", bathroom_count) # Int

            # # item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            # item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
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
            # # #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", currency) # String

            # # #item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # # #item_loader.add_value("energy_label", energy_label) # String

            # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            yield item_loader.load_item()
