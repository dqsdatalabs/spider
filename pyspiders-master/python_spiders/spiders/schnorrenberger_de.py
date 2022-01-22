# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json 
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Schnorrenberger_deSpider(Spider):
    name = 'schnorrenberger_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.schnorrenberger.de"]
    start_urls = ["https://www.schnorrenberger.de/angebote/?mt=rent&address&sort=meta_value%7Cdesc"]
    position = 1

    def parse(self, response):
        for url in response.css("div.immolisting div.wp-block-column a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.wp-block-group__inner-container h1::text").get()

        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
        ):
            return
        
        rent = response.css("div.immo-expose__head--price h2::text").get()
        if(rent):
            rent = rent.split(",")[0]
        else:
            return
        rent = re.findall(r"([0-9]+)", rent)
        rent = "".join(rent)
        if(not re.search("([0-9]+)", rent)):
            return
        
        currency = "EUR"

        utilities = response.css("span.key:contains('Nebenkosten') + span.value::text").get()
        heating_cost = response.css("span.key:contains('Heizkosten') + span.value::text").get()
        deposit = response.css("div.col-12:contains('Kaution') + div.col-12::text").get()

        room_count = response.css("ul.immo-expose__head--iconFields div:contains('Zimmer') h3.value::text").get()
        if(room_count):
            if("," in room_count):
                room_count = room_count.split(",")
                room_count = ".".join(room_count)
                room_count = str(math.ceil(float(room_count)))
            if( not re.search(r"([1-9])", room_count)):
                room_count = "1"
        else:
            room_count = "1"
        square_meters = response.css("div:contains('Wohnfläche') h3.value::text").get()
        if(not square_meters):
            square_meters = response.css("div:contains('Gesamtfläche') h3.value::text").get()        
        if(not square_meters):
            square_meters = response.css("div:contains('Nutzfläche') h3.value::text").get()

        external_id = response.css("span.key:contains('Objekt-Nr') + span.value::text").get()
        floor = response.css("span.key:contains('Etage') + span.value::text").get()
        available_date = response.css("span.key:contains('verfügbar ab') + span.value::text").get()

        tabs = response.css("div.vue-tabs::attr(data-tabs)").get()
        tabs = json.loads(tabs)
        description = None
        for tab in tabs:
            try:
                description = tab["rawValue"]
                break
            except:
                pass

        address = response.css("div.immo-expose__list-price div.row div.col-24:contains('Standort') + div.col-24 p::text").get()
        try:
            location_data = extract_location_from_address(address)
            latitude = str(location_data[1])
            longitude = str(location_data[0])
        except:
            latitude = None
            longitude = None

        if( latitude and longitude):
            try:
                location_data = extract_location_from_coordinates(longitude, latitude)
                city = location_data[1]
                zipcode = location_data[0]
            except:
                city = title.split(" in ")[1]
                zipcode = None

        images = response.css("div.lightgallery a::attr(href)").getall()

        landlord_name = "schnorrenberger"
        landlord_phone = "0211 580 50 50"
        landlord_email = "info@schnorrenberger.de"

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
        # # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
