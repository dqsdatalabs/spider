# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup
import datetime
import dateparser


class WgaufbauDresdenSpider(scrapy.Spider):
    name = "wgaufbau_dresden"
    start_urls = ['https://www.wgaufbau-dresden.de/objektsuche/wohnung/']
    allowed_domains = ["wgaufbau-dresden.de"]
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        property_urls = response.css('.yellow-button::attr(href)').extract()
        property_urls = ['https://www.wgaufbau-dresden.de/objektsuche/wohnung/' + x for x in property_urls]
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1 ::text')[0].extract()
        list = response.css('#immo-details-tab-infos div ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        if 'Kaltmiete ' in list:
            rent = list.split('Kaltmiete ')[1].split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        utilities = None
        if 'Betriebskosten ' in list:
            utilities = list.split('Betriebskosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        heating_cost = None
        if 'Heizkosten ' in list:
            heating_cost = list.split('Heizkosten ')[1].split(',')[0]
            heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
        deposit = None
        if 'Eintrittsgeld ' in list:
            deposit = list.split('Eintrittsgeld ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        floor = None
        if 'Etage ' in list:
            floor = list.split('Etage ')[1].split(' Etagen')[0]
        address = None
        if 'Adresse ' in list:
            address = list.split('Adresse ')[1].split(' Typ')[0]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, ad = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        external_id = None
        if 'Wohnungsnummer ' in list:
            external_id = list.split('Wohnungsnummer ')[1].split(' ')[0]
        energy_label = None
        if 'Energieeffizienzklasse ' in list:
            energy_label = list.split('Energieeffizienzklasse ')[1].split(' ')[0]
        elif 'Energiekennwert ' in list:
            energy_label = int(list.split('Energiekennwert ')[1].split(',')[0])
            if energy_label >= 250:
                energy_label = 'H'
            elif energy_label >= 200 and energy_label <= 250:
                energy_label = 'G'
            elif energy_label >= 150 and energy_label <= 200:
                energy_label = 'F'
            elif energy_label >= 125 and energy_label <= 150:
                energy_label = 'E'
            elif energy_label >= 100 and energy_label <= 125:
                energy_label = 'D'
            elif energy_label >= 75 and energy_label <= 100:
                energy_label = 'C'
            elif energy_label >= 50 and energy_label <= 75:
                energy_label = 'B'
            elif energy_label >= 25 and energy_label <= 50:
                energy_label = 'A'
            elif energy_label >= 1 and energy_label <= 25:
                energy_label = 'A+'
        room_count = 1
        if 'Zimmer ' in list:
            room_count = list.split('Zimmer ')[1].split(' ')[0]
        square_meters = None
        if 'Wohnfläche ' in list:
            square_meters = list.split('Wohnfläche ')[1].split(',')[0]
            square_meters = int(square_meters) + 1

        parking = None
        if 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in list.lower():
            dishwasher = True
        terrace = None
        if 'terras' in list.lower():
            terrace = True
        elevator = None
        if 'aufzug' in list.lower():
            elevator = True
        balcony = None
        if 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in list.lower():
            furnished = True

        images = response.css('.carousel-item a::attr(href)').extract()

        if int(rent) <= 0 and int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        #item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", 1) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_name", 'Wgaufbau Dresden') # String
        item_loader.add_value("landlord_phone", '0351 4432-0') # String
        item_loader.add_value("landlord_email", 'info@wga-dresden.de') # String

        self.position += 1
        yield item_loader.load_item()
