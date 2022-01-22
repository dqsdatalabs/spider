# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Falcimmo_deSpider(Spider):
    name = 'falcimmo_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.falcimmo.de"]
    start_urls = ["https://www.falcimmo.de/mietwohnungen.html"]
    position = 1

    def parse(self, response):
        for url in response.css("div.main-image a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("li.next a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.expose_mod_title h1::text").get()
        rent = response.css("div[title='Kaltmiete']::text").get()
        if(not re.search("([0-9]+)", rent)):
            return
        
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        address = response.css("div.expose_mod_addressFalc address::text").get()
        
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("span.label:contains('Objektnr.') + span.value::text").get()

        images = response.css("div.gallery-item img::attr(src)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        room_count = response.css("div.label:contains('Zimmer:') + div.value::text").get()
        bathroom_count = response.css("div.label:contains('Badezimmer:') + div.value::text").get()
        square_meters = response.css("div.label:contains('Wohnfläche:') + div.value::text").get()
        if(square_meters):
            if("," in square_meters):
                square_meters = square_meters.split(",")[0]

        balcony = response.css("div.label:contains('Balkone:') + div.value::text").get()
        if(balcony):
            balcony = True
        else:
            balcony = False
        
        floor = response.css("div.label:contains('Etage:') + div.value::text").get()
        
        terrace = response.css("div.label:contains('Terassenfläche:') + div.value::text").get()
        if(terrace):
            terrace = True
        else: 
            terrace = False

        elevator = response.css("div.label:contains('Fahrstuhlart:') + div.value::text").get()
        if(elevator):
            elevator = True
        else: 
            elevator = False
        
        furnished = response.css("div.label:contains('Ausstattung:') + div.value::text").get()
        if( furnished):
            furnished = True
        else:
            furnished = False
        
        utilities = response.css("div.label:contains('Nebenkosten:') + div.value::text").get()
        if(utilities):
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)
        deposit = response.css("div.label:contains('Kaution:') + div.value::text").get()
        if(deposit):
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        description = response.css("div.tab_objektbeschreibung div.tab-content p::text").getall()
        description = " ".join(description)
        energy_label = response.css("div.label:contains('Effizienzklasse:') + div.value::text").get()

        landlord_name = response.css("div.expose_mod_contactPerson").css("span.name::text").get()
        landlord_phone = response.css("div.expose_mod_contactPerson").css("span.phone a::text").get()
        landlord_email = response.css("div.expose_mod_contactPerson").css("span.email a::attr(title)").get()

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

        # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
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
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
