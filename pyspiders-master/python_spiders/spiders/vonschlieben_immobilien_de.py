# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Vonschlieben_immobilien_deSpider(Spider):
    name = 'vonschlieben_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.vonschlieben-immobilien.de"]
    start_urls = ["https://www.vonschlieben-immobilien.de/immobilienangebote?c=wohnung-mieten"]
    position = 1

    def parse(self, response):
        for url in response.css("div.immo-col a.ablow::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("ul.pagination li a::attr(href)").getall()[-1]
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.pull-left::text").get()
        if("RESERVIERT" in title):
            return
        
        if("VERMIETET" in title):
            return

        images = response.css("div.image-navigation img.fancybox-img::attr(src)").getall()
        external_id = response.css("div:contains('Immo-ID:') b::text").get()
        address = response.css("h3:contains('Objektanschrift') + div div::text").getall()
        address = " ".join(address)
        address = re.sub("\s+", " ", address)

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        warm_rent = response.css("div.immo-field-label:contains('Warmmiete:') + div.immo-field-value::text").get()
        if(warm_rent):
            warm_rent = warm_rent.split(",")[0]
            warm_rent = re.findall("([0-9]+)", warm_rent)
            warm_rent = "".join(warm_rent)
        else:
            warm_rent = "0"
        
        cold_rent = response.css("div.immo-field-label:contains('Kaltmiete:') + div.immo-field-value::text").get()
        if(cold_rent):
            cold_rent = cold_rent.split(",")[0]
            cold_rent = re.findall("([0-9]+)", cold_rent)
            cold_rent = "".join(cold_rent)
        else:
            cold_rent = "0"

        rent = None
        if(int(warm_rent) > int(cold_rent)):
            rent = warm_rent
        else:
            rent = cold_rent
        
        currency = "EUR"

        utilities = response.css("div.immo-field-label:contains('Nebenkosten:') + div.immo-field-value::text").get() 
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)
        else:
            utilities = "0"
        
        deposit = response.css("div.immo-field-label:contains('Kaution:') + div.immo-field-value::text").get() 
        if(deposit):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)
        else:
            deposit = "0"

        square_meters = response.css("div.immo-field-label:contains('Wohnfläche ca. (m²):') + div.immo-field-value::text").get() 
        room_count = response.css("div.immo-field-label:contains('Anzahl der Zimmer:') + div.immo-field-value::text").get() 
    
        balcony = response.css("div.immo-field-label:contains('Anzahl der Balkone:') + div.immo-field-value::text").get() 
        if(balcony):
            balcony = True
        else:
            balcony = False
    
        energy_label = response.css("div.immo-field-label:contains('Energieausweis (Effizienzklasse):') + div.immo-field-value::text").get() 
        floor = response.css("div.immo-field-label:contains('Etage:') + div.immo-field-value::text").get() 
        description = response.css("h3:contains('Objektbeschreibung') + div p.text14::text").getall()
        description = " ".join(description)

        landlord_name = "vonschlieben-immobilien"
        landlord_phone = "0621 121 81 620"
        landlord_email = "mail@vonschlieben-immobilien.de"

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
        # # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
