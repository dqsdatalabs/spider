# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Clvgroup_comSpider(Spider):
    name = 'clvgroup_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.clvgroup.com"]
    start_urls = ["https://www.clvgroup.com/"]
    position = 1

    def parse(self, response):
        for url in response.css("ul.child-pages li.nav-child a.child-link::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages, meta = {"city": url.split("/")[-1]})

    def get_pages(self, response):
        for url in response.css("div.property-item::attr(data-path)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta = response.meta)

    def populate_item(self, response):
        
        property_type = "apartment"
        city = response.meta.get("city")
        title = response.css("h1::text").get()
        currency = "CAD"

        address = response.css("span.address::text").get()
        try:
            location_data = extract_location_from_address(address)
            latitude = str(location_data[1])
            longitude = str(location_data[0])
        except:
            latitude = None
            longitude = None  

        zipcode = None
        if(latitude and longitude):
            location_data = extract_location_from_coordinates(longitude, latitude)
            zipcode = location_data[0]

        images = response.css("div.gallery-image div.cover::attr(style)").getall()

        images = [ image_src.split("background-image:url('")[1].split("');")[0] for image_src in images]
        description = response.css("div.property-description p::text").getall()
        description = " ".join(description)

        amenities = response.css("section.amenities div ul li::text").getall()
        amenities = " ".join(amenities)
        
        balcony = "Balcony" in amenities
        elevator = "Elevators" in amenities
        dishwasher = "Dishwasher" in amenities
        washing_machine = "Laundry" in amenities
        pets_allowed = "Pet Friendly" in amenities
        swimming_pool = "Pool" in amenities
        parking = "Parking" in amenities

        external_id = response.css("a.favorite_properties::attr(data-property-id)").get()

        landlord_name = "clvgroup"
        landlord_phone = "226-486-2291"
        landlord_email = "info@clvgroup.com"

        property_record = 1
        suites  = response.css("div.suite")
        for suite in suites:
            rent = suite.css("span.rate::text").get()
            if(not re.search("([0-9]+)", rent)):
                continue
            
            rent = re.findall("([0-9]+)", rent)
            rent = "".join(rent)
            floor_plan_images = suite.css("a.floorplan-link::attr(href)").get()
            rooms = suite.css("div.bed-bath span::text").getall()
            rooms = " ".join(rooms)
            
            room_count = re.findall("([0-9])  Bedroom", rooms)
            if(len(room_count) > 0):
                room_count = room_count[0]
            else:
                room_count = "1"

            bathroom_count = re.findall("([0-9])  Bathroom", rooms)
            if(len(bathroom_count) > 0):    
                bathroom_count = bathroom_count[0]
            else:
                bathroom_count = None

            available_date = suite.css("div.suite-date::text").get().strip()

            item_loader = ListingLoader(response=response)
            # # # MetaData
            item_loader.add_value("external_link", f"{response.url}#{property_record}") # String
            property_record +=1
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
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            # # #item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
