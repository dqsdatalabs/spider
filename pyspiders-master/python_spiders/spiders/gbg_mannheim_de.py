# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gbg_mannheim_deSpider(Spider):
    name = 'gbg_mannheim_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.gbg-mannheim.de"]
    start_urls = ["https://gbgmh.immomio.online/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.property-actions a:contains('Details')::attr(href)").getall():
            if("parken" in url):
                continue 
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.property-title::text").get()

        rent = response.css("div.dt:contains('Kaltmiete') + div.dd::text").get()
        rent = rent.split(",")[0]
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        address = response.css("div.dt:contains('Adresse') + div.dd::text").getall()
        address = " ".join(address)
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("div.dt:contains('Objekt ID') + div.dd::text").get()
        floor = response.css("div.dt:contains('Etage') + div.dd::text").get()
        square_meters = response.css("div.dt:contains('Wohnfläche') + div.dd::text").get()
        square_meters = square_meters.split(",")[0]

        room_count = response.css("div.dt:contains('Zimmer') + div.dd::text").get()
        bathroom_count = response.css("div.dt:contains('Badezimmer') + div.dd::text").get()
        available_date = response.css("div.dt:contains('Verfügbar ab') + div.dd::text").get()
        deposit = response.css("div.dt:contains('Kaution') + div.dd::text").get()
        if(deposit != None):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        heating_cost = response.css("div.dt:contains('Heizkosten') + div.dd::text").get()
        if(heating_cost != None):
            heating_cost = heating_cost.split(",")[0]
            heating_cost = re.findall("([0-9]+)", heating_cost)
            heating_cost = "".join(heating_cost)

        utilities = response.css("div.dt:contains('Nebenkosten') + div.dd::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)


        amenities = response.css("li.list-group-item::text").getall()
        amenities = " ".join(amenities).strip()

        terrace = "Terrasse" in amenities
        balcony = "Balkon" in amenities

        energy_label = response.css("div.dt:contains('Energie­effizienz­klasse') + div.dd::text").get()
        
        images = response.css("div#immomakler-galleria a::attr(href)").getall()
        
        description = response.css("div.panel-body p::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        landlord_name = "gbg-mannheim"
        landlord_phone = "0621-3096-0"


        item_loader = ListingLoader(response=response)
        # # # MetaData
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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
