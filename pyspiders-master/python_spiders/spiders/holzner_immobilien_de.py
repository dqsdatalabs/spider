# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Holzner_immobilien_deSpider(Spider):
    name = 'holzner_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.holzner-immobilien.de"]
    start_urls = ["https://www.holzner-immobilien.de/vermieten.xhtml?f[5352-28]=miete"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Exposé ansehen')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("a:contains('»')::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)


    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h3.title::text").get()
        cold_rent = response.css("td:contains('Kaltmiete') + td span::text").get()
        warm_rent = response.css("td:contains('Warmmiete') + td span::text").get()

        rent = None
        if( not cold_rent ):
            cold_rent = "0"
        
        if( not warm_rent):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not rent):
            return
        
        currency = "EUR"

        description = response.css("div.freitexte p::text").getall()
        description = " ".join(description)
        if("vermietet" in description):
            return
        external_id = response.css("td:contains('externe Objnr') + td span::text").get()
        room_count = response.css("td:contains('Anzahl Zimmer/Räume') + td span::text").get()
        if(room_count):
            room_count = re.findall("([1-9])", room_count)[0]
            if(not re.search(r"([1-9])", room_count)):
                room_count = "1"
            
        else:
            room_count = "1"
        bathroom_count = response.css("td:contains('Anzahl Badezimmer') + td span::text").get()
        
        square_meters = response.css("td:contains('Wohnfläche') + td span::text").get()
        if(not square_meters):
            square_meters = response.css("td:contains('Nutzfläche') + td span::text").get()

        floor = response.css("td:contains('Etage') + td span::text").get()
        
        utilities = response.css("td:contains('Nebenkosten') + td span::text").get()
        utilities = re.findall(r"([0-9]+)", utilities)
        utilities = "".join(utilities)
        
        deposit = response.css("td:contains('Kaution') + td span::text").get()
        deposit = re.findall(r"([0-9]+)", deposit)
        deposit = "".join(deposit)
        
        balcony = "Balkon" in description
        terrace = response.css("td:contains('Terrasse') + td span::text").get()
        if( terrace == "Ja"):
            terrace = True
        else:
            terrace = False
        
        images = response.css("ul li a img::attr(src)").getall()
        images = [image_src.split("@")[0] for image_src in images]

        landlord_name = "holzner-immobilien"
        landlord_phone = response.css("strong:contains('Telefon:') + span::text").get()
        landlord_email = response.css("strong:contains('E-Mail:') + span a::text").get()

        city = response.css("td:contains('Ort') + td span::text").get()

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
        # item_loader.add_value("zipcode", zipcode) # String
        # item_loader.add_value("address", address) # String
        # item_loader.add_value("latitude", latitude) # String
        # item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

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
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
