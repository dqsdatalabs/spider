# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from datetime import datetime
from datetime import date
import dateparser

class ImmoStroblSpider(scrapy.Spider):
    name = "immo_strobl"
    start_urls = ['https://www.immobilien-strobl.de/immobilien/mieten/']
    country = 'Germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_urls = response.css('.listEntryTitle a::attr(href)').extract()
        property_urls = ['https://www.immobilien-strobl.de/' + x for x in property_urls]
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("h1::text")[0].extract()
        if 'Büro-Ensemble' in title:
            return
        elif 'Haus' in title:
            property_type = 'house'
        else:
            property_type = 'apartment'
        external_id = response.css(".objectIdValue::text")[0].extract()
        description = response.css(".elementObjectContent_Objektbeschreibung p::text")[0].extract()

        address = response.css(".h5::text")[0].extract()
        longitude,latitude = extract_location_from_address(address)
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        list = response.css("dt::text").extract()
        rent_index = [i for i, x in enumerate(list) if 'Warmmiete' in x][0]
        rent = response.css("dd::text")[rent_index].extract()
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        deposit_index = [i for i, x in enumerate(list) if 'Kaution' in x][0]
        deposit = response.css("dd::text")[deposit_index].extract()
        if any(char.isdigit() for char in deposit):
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities_index = [i for i, x in enumerate(list) if 'Nebenkosten' in x][0]
        utilities = response.css("dd::text")[utilities_index].extract()
        if any(char.isdigit() for char in utilities):
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        room_index = [i for i, x in enumerate(list) if 'Schlafzimmer' in x][0]
        room_count = response.css("dd::text")[room_index].extract()
        if ',' in room_count:
            room_count = int(room_count[0]) + 1
        else:
            room_count = int(room_count[0])
        bathroom_index = [i for i, x in enumerate(list) if 'Badezimmer' in x][0]
        bathroom_count = response.css("dd::text")[bathroom_index].extract()
        bathroom_count = int(bathroom_count)
        try:
            square_index = [i for i, x in enumerate(list) if 'Gesamtfläche (ca.)' in x][0]
            square_meters = response.css("dd::text")[square_index].extract()
            square_meters = square_meters[:-2]
            if ',' in square_meters:
                square_meters = square_meters[:-4]
            square_meters = int(square_meters)
        except:
            square_index = [i for i, x in enumerate(list) if 'Wohnfläche (ca.)' in x][0]
            square_meters = response.css("dd::text")[square_index].extract()
            square_meters = square_meters[:-2]
            if ',' in square_meters:
                square_meters = square_meters[:-4]
            square_meters = int(square_meters)
        heating_index = [i for i, x in enumerate(list) if 'Nebenkosten' in x][0]
        heating_cost = response.css("dd::text")[heating_index].extract()
        if any(char.isdigit() for char in heating_cost):
            heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))

        elevator = None
        try:
            elevator_index = [i for i, x in enumerate(list) if 'Aufzug' in x][0]
            elevator = response.css("dd::text")[elevator_index].extract()
            if 'ja' in elevator:
                elevator = True
        except:
            pass
        floor = None
        try:
            floor_index = [i for i, x in enumerate(list) if 'Anzahl Etagen' in x][0]
            floor = response.css("dd::text")[floor_index].extract()
        except:
            pass
        available_date = None
        try:
            date_index = [i for i, x in enumerate(list) if 'verfügbar ab' in x][0]
            available_date = response.css("dd::text")[date_index].extract().strip()
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%d-%m")
        except:
            pass
        energy_label = None
        try:
            energy_index = [i for i, x in enumerate(list) if 'Energieeffizienzklasse' in x][0]
            energy_label = response.css("dd::text")[energy_index].extract().strip()
        except:
            pass
        furnished = None
        try:
            furnished = response.css("#blockContent li::text").extract()
            if furnished:
                furnished = True
        except:
            pass
        terrace = None
        try:
            if 'Großzügige Dachterrasse' in furnished:
                terrace = True
        except:
            pass

        images = response.css(".listEntry::attr(data-src)").extract()
        images = ['https://www.immobilien-strobl.de/' + x for x in images]

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
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

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Strobl Immobilien") # String
        item_loader.add_value("landlord_phone", "+49 89 641881-0") # String
        item_loader.add_value("landlord_email", "info@immobilien-strobl.de") # String

        self.position += 1
        yield item_loader.load_item()
