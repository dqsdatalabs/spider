# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gesobau_deSpider(Spider):
    name = 'bratek_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.bratek-immobilien.de"]
    start_urls = ["https://www.bratek-immobilien.de/unternehmen/referenzen/"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Zum Exposé')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
            
        next_page = response.css("a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.mt-2::text").getall()
        title = "".join(title).strip()
        if("Kauf" in title):
            return
        
        rent = response.css("li:contains('Kaltmiete') span.float-right::text").get()
        if(not rent):
            return
        if("," in rent):
            rent = rent.split(",")[0]
        rent = re.findall("([0-9]+)",rent)
        rent = "".join(rent)
        currency = "EUR"

        utilities = response.css("li:contains('Nebenkosten') span.float-right::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)",utilities)
            utilities = "".join(utilities)
        
        deposit = response.css("div:contains('Kaution') + div::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)
        
        address = response.css("div:contains('Adresse') + div ul li::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]
        
        external_id = response.css("li:contains('Objekt-Nr') span.float-right::text").get()

        floor = response.css("li:contains('Etage') span.float-right::text").get()
        available_date = response.css("li:contains('verfügbar ab') span.float-right::text").get()
        square_meters = response.css("li:contains('Wohnfläche') span.float-right::text").get()
        if( not square_meters):
            square_meters = response.css("li:contains('Nutzfläche') span.float-right::text").get()
        if( not square_meters):
            square_meters = response.css("li:contains('Gartenfläche') span.float-right::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]

        room_count = response.css("li:contains('Zimmer') span.float-right::text").get()
        if(room_count):
            if("," in room_count):
                room_count = room_count.split(",")
                room_count = ".".join(room_count)
            room_count = str(math.ceil(float(room_count)))
            if(not re.search(r"([1-9])", room_count)):
                room_count = "1"
        else:
            room_count = "1" 
        bathroom_count = response.css("li:contains('Anzahl separate WCs') span.float-right::text").get()

        balcony = response.css("li:contains('Balkon') span.float-right::text").get()
        if(balcony):
            balcony = True
        else:
            balcony = False

        terrace = response.css("li:contains('Terrasse') span.float-right::text").get()
        if(terrace):
            terrace = True
        else:
            terrace = False

        amenities = response.css("ul.expose__check-overview li::text").getall()
        amenities = " ".join(amenities)

        washing_machine = "Wasch/Trockenraum" in amenities
        elevator = "Fahrstuhl" in amenities

        description = response.css("script.vue-tabs::text").getall()
        description = " ".join(description)
        description = re.findall("<p>(.+)</p>", description)
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

    
        images = response.css("div#exGallery a::attr(href)").getall()
        energy_label = response.css("ul.expose__list li:contains('Energieeffizienzklasse') span::text").get()

        landlord_name = "bratek-immobilien"
        landlord_phone = "+49 711 3424350"

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
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
