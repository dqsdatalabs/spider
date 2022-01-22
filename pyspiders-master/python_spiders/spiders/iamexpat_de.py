# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces
from python_spiders.user_agents import random_user_agent

class Iamexpat_deSpider(Spider):
    name = 'iamexpat_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.iamexpat.de"]
    start_urls = ["https://www.iamexpat.de/"]
    position = 1

    custom_settings = {
        "User-Agent": random_user_agent()
    }

    def parse(self, response):
        for url in response.css("a:contains('Find a rental property')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages)

    def get_pages(self, response):
        for url in response.css("div.typography a.article__link::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("li.pager-next a::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.article__title::text").get()
        rent = response.css("span.property__price::text").get()
        if(not re.search("([0-9]+)", rent)):
            return
        
        currency = "EUR"

        square_meters = response.css("div.field:contains('Surface:')::text").getall()
        if(len(square_meters) > 0):
            square_meters = "".join(square_meters)
            square_meters = re.findall("([0-9]+)", square_meters)
            square_meters = "".join(square_meters)
            square_meters = int(square_meters)
        else: 
            square_meters = None

        room_count = response.css("div.field:contains('Bedrooms:')::text").getall()
        if(len(room_count) > 0):
            room_count = "".join(room_count)
        else: 
            room_count = "1"

        furnished = response.css("div.field:contains('Interior:')::text").getall()
        if(len(furnished) > 0):
            furnished = "".join(furnished)
            if("Yes" in furnished):
                furnished = True
            else:
                furnished = False
        else: 
            furnished = None

        available_date = response.css("div.field:contains('Available From:')::text").getall()
        if(len(available_date) > 0):
            available_date = "".join(available_date)
            available_date = available_date.strip()
        else: 
            available_date = None
        
        if(available_date):
            available_date = available_date.split(" ")
            available_date = f"{available_date[2]}-{available_date[1]}-{available_date[0]}"

        address = response.css("div.field:contains('Address:')::text").getall()
        if(len(address) > 0):
            address = "".join(address)
        else: 
            address = None

        city = response.css("div.field:contains('Neighbourhood:')::text").getall()
        if(len(city) > 0):
            city = "".join(city)
        else: 
            city = None

        if(city):
            address = f"{address}, {city}"
            address = re.sub("\s+", " ", address)


        deposit = response.css("div.field:contains('Deposit:')::text").getall()
        if(len(deposit) > 0):
            deposit = "".join(deposit)
        else: 
            deposit = None

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        zipcode = location_data[0]
        city = location_data[1]
        address = location_data[2]

    
        pets_allowed = response.css("div.field:contains('Pets:')::text").getall()
        if(len(pets_allowed) > 0):
            pets_allowed = "".join(pets_allowed)
            if("Allowed" in pets_allowed):
                pets_allowed = True
            else:
                pets_allowed = False
        else: 
            pets_allowed = None

        bathroom_count = response.css("div.field:contains('Bathrooms:')::text").getall()
        if(len(bathroom_count) > 0):
            bathroom_count = "".join(bathroom_count)
        else: 
            bathroom_count = None
        
        parking = response.css("div.field:contains('Parking:')::text").getall()
        if(len(parking) > 0):
            parking = "".join(parking)
            if("Yes" in parking):
                parking = True
            else:
                parking = False
        else:
            parking = False

        description = response.css("div.property__left-section::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        description = description.replace("-", "")

        if( not parking ):
            parking = "parking" in description.lower()
        
        if(not furnished):
            furnished = "furnished" in description.lower()

        images = response.css("source::attr(srcset)").getall()
        images_to_add = {}
        for image_src in images:
            image_src = image_src.split(" ")[0]
            if("image_gallery_medium_custom_user_desktop_2x/public/" in image_src):
                images_to_add[image_src] = image_src

        images = []
        for image_src in images_to_add:
            images.append(images_to_add[image_src])

        landlord_name = "iamexpat"
        landlord_email = "info@iamexpat.de"

        washing_machine = "laundry" in description.lower()
        dishwasher = "dishwasher" in description.lower()
        elevator = "elevator" in description.lower()

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
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
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        # item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
