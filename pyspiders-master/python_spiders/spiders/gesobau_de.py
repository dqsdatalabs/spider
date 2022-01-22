# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gesobau_deSpider(Spider):
    name = 'gesobau_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.gesobau.de"]
    start_urls = ["https://www.gesobau.de/"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Wohnungssuche')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages)

    def get_pages(self, response):
        for url in response.css("div.list_item-body h3.list_item-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("li.next a::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("header h1::text").get()
        details = response.css("div.facts_summary p.details span::text").getall()
        if(len(details) == 0):
            return
        rent = details[0]
        rent = rent.split("inkl. NK:")[1]
        rent = rent.split(",")[0]
        currency = "EUR"

        square_meters = details[1]
        square_meters = square_meters.split("Wohnfläche: ")[1]
        square_meters = square_meters.split(",")[0]

        room_count = details[2]
        room_count = room_count.split("Anzahl Zimmer:")[1]
        if("," in room_count):
            room_count = room_count.split(",")[0]
        room_count = int(float(room_count))

        address = response.css("div.facts_summary p.location::text").get()

        description = response.css("header:contains('Objektbeschreibung') + div p::text").getall()
        description = " ".join(description)

        heating_cost = response.css("dt:contains('Heizkosten') + dd p::text").get()
        heating_cost = heating_cost.split(",")[0] 
        
        deposit = response.css("dt:contains('Kaution') + dd p::text").get()
        deposit = deposit.split(",")[0]
        deposit = re.findall("([0-9]+)", deposit)
        deposit = "".join(deposit)

        amenities = response.css("ul.gesobau_ul li::text").getall()
        amenities = " ".join(amenities).lower()

        balcony = "balkon" in amenities
        
        floor = response.css("dt:contains('Etage') + dd p::text").get()
        external_id = response.css("dt:contains('Objektnummer') + dd p::text").get()

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("div.slide-item img::attr(src)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        landlord_name = "gesobau"
        landlord_phone = "(030) 4073 - 0"
        landlord_email = "vermietung@gesobau.de"

        available_date = response.css("dt:contains('Frei ab') + dd p::text").get()
        
        utilities = response.css("dt:contains('Betriebskosten') + dd p::text").get()
        if(utilities):
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
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
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

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
