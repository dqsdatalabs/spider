# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates

class Web_frankfurtrentals_deSpider(Spider):
    name = 'web_frankfurtrentals_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.web.frankfurtrentals.de"]
    start_urls = ["https://web.frankfurtrentals.de/en/"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Rent offers')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages, dont_filter = True)

    def get_pages(self, response):
        for url in response.css("ul.pagination li.page-item a.page-link::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.get_property_page, dont_filter = True)
        
    def get_property_page(self, response):
        for url in response.css("div.object-list-div-text-lg:contains('for rent') a.object-list-link::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.populate_item, dont_filter = True)


    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1#property-title::text").get()
        cold_rent = response.css("p:contains('Monthly cold rent in EUR:')::text").get()
        warm_rent = response.css("p:contains('Monthly warm rent in EUR:')::text").get()
        rent = None
        if( cold_rent ):
            cold_rent = cold_rent.split(".")[0]
        else:
            cold_rent = "0"
        
        if(warm_rent):
            warm_rent = warm_rent.split(".")[0]
        else:
            warm_rent = "0"

        cold_rent = re.findall(f"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(f"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = warm_rent
        else: 
            rent = cold_rent
        
        if(not rent):
            return
        currency = "EUR"

        available_date = response.css("p:contains('Available as of:')::text").get()
        zipcode = response.css("p:contains('ZIP Code:')::text").get()
        city = response.css("p:contains('City or town:')::text").get()
        town = response.css("p:contains('Part of City:')::text").get()

        address = f"{city}, {zipcode}, {town}"
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        square_meters = response.css("p:contains('Size living space sqm approx.:')::text").get()
        square_meters = square_meters.split(".")[0]

        description = response.css("p.data-type-textarea::text").getall()
        description = " ".join(description)
        utilities =  response.css("p:contains('Utilities monthly prorated in EUR:')::text").get()
        if( utilities ):
            utilities = utilities.split(".")[0]
        
        energy_label =  response.css("p:contains('Energy efficiency class:')::text").get()
        if(energy_label):
            energy_label = re.findall("([A-Z])", energy_label)
            if(len(energy_label) > 0):
                energy_label = energy_label[0]
            else: 
                energy_label = None
        
        room_count =  response.css("p:contains('Total number of rooms:')::text").get()
        if( room_count ):
            if("." in room_count):
                room_count = int(float(room_count))
            room_count = int(float(room_count))
        else:
            room_count = "1"

        bathroom_count =  response.css("p:contains('Number of bathrooms:')::text").get()
        floor =  response.css("p:contains('Floor:')::text").get()
        furnished =  response.css("p:contains('Furnishing:')::text").get()
        if( furnished):
            if("unfurnished" in furnished):
                furnished = False
            
            if(furnished and "furnished" in furnished):
                furnished = True

        images = response.css("img.lazyload::attr(data-src)").getall()

        landlord_name = "frankfurtrentals"
        landlord_phone = "+49 6103 310847"
        landlord_email = "rentals@allgrund.com"

        external_id = response.css("input#property_id::attr(value)").get()

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # #item_loader.add_value("elevator", elevator) # Boolean
        # #item_loader.add_value("balcony", balcony) # Boolean
        # #item_loader.add_value("terrace", terrace) # Boolean
        # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # #item_loader.add_value("washing_machine", washing_machine) # Boolean
        # #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent_string", str(rent)) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # #item_loader.add_value("water_cost", water_cost) # Int
        # #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
