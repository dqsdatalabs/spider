# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gesobau_deSpider(Spider):
    name = 'immobiliareabissinia_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliareabissinia.com"]
    start_urls = ["https://www.immobiliareabissinia.com/property-search/?status=affitto"]
    position = 1

    def parse(self, response):
        for url in response.css("article.property-item h4 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.populate_item)
        
        next_page = response.css("a.rh_arrows_right::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("address.title::text").get().strip()
        rent = response.css("span.price-and-type::text").getall()
        rent = "".join(rent).strip()
        currency = "EUR"
        if(not re.search("([0-9]+)", rent)):
            return

        external_id = response.css("span[title='ID Immobile']::text").get()
        square_meters = response.css("span[title='Superficie']::text").get()
        
        room_count = response.css("span.property-meta-bedrooms::text").get()
        if(room_count):
            room_count = re.findall("([0-9]+)", room_count)
            if(len(room_count) > 0):
                room_count = room_count[0]
        else:
            room_count = "1"
        
        bathroom_count = response.css("span.property-meta-bath::text").get()
        description = response.css("div.content p::text").getall()
        description = " ".join(description)

        features = response.css("div.features ul li a::text").getall()
        features = " ".join(features).lower()

        elevator = "ascensore" in features
        parking = "posto auto" in features
        furnished = "arreda" in features

        images = response.css("ul.slides li a.swipebox::attr(href)").getall()

        try:

            location_script = response.css("script#property-google-map-js-extra::text").get()

            latitude = re.findall('"lat":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
            longitude = re.findall('"lng":"(-?[0-9]+\.[0-9]+)"', location_script)[0]

            location_data = extract_location_from_coordinates(longitude, latitude)
            zipcode = location_data[0]
            city = location_data[1]
            address = location_data[2]
        except:
            address = title
            location_data = extract_location_from_address(address)
            longitude = str(location_data[0])
            latitude = str(location_data[1])
            
            location_data = extract_location_from_coordinates(longitude, latitude)
            city = location_data[1]
            zipcode = location_data[0]


        landlord_name = "immobiliareabissinia"
        landlord_phone = "+39.0541.414231"
        landlord_email = "info@immobiliareabissinia.com"

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
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

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
