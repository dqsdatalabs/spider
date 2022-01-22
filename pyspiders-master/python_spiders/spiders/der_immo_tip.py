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


class DerImmoTipSpider(scrapy.Spider):
    name = "der_immo_tip"
    start_urls = ['https://www.der-immo-tip.de/angebote/wohnen/mieten/wohnungen/seite/1/']
    allowed_domains = ["der-immo-tip.de"]
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
        property_urls = response.css('.linkboxColor ::attr(href)').extract()
        property_urls = ['https://www.der-immo-tip.de/' + x for x in property_urls]
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)
        last_page = response.css('.pagebrowser-last .act-2 ::attr(href)')[0].extract()
        current_page = response.url
        page_next = response.css('.pagebrowser-next .act-2 ::attr(href)')[0].extract()
        if current_page != last_page:
            yield Request(url='https://www.der-immo-tip.de/' + page_next, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1 ::text')[0].extract()
        address = response.css('tr:nth-child(2) .value ::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        latitude = str(latitude)
        longitude = str(longitude)
        description = response.css('.nb-maklerTool-expose-box > .accContent ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        list = response.css('.exposeSecBody td ::text').extract()
        list = ' '.join(list)
        if 'Nettokaltmiete' in list:
            rent = list.split('Nettokaltmiete: ')[1].split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        deposit = None
        if 'Kaution' in list:
            deposit = list.split('Kaution: ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten: ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))

        external_id = None
        if 'Objekt-ID' in list:
            external_id = list.split('Objekt-ID: ')[1].split(' ')[0]
        room_count = 1
        floor = None
        if 'Etage' in list:
            floor = list.split('Etage: ')[1].split(' ')[0]
        room_count = 1
        if 'Zimmer' in list:
            room_count = int(list.split('Zimmer: ')[1].split(' ')[0])
        square_meters = None
        if 'Wohnfläche' in list:
            square_meters = int(list.split('Wohnfläche:  ca. ')[1].split(' ')[0])
        available_date = None
        if 'Bezugstermin' in list:
            available_date = list.split('Bezugstermin: ')[1].split(' ')[0]
            try:
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

        bathroom_count = None
        if 'bad' in title.lower() or 'bad' in description.lower():
            bathroom_count = 1
        balcony = None
        if 'balkon' in title.lower() or 'balkon' in description.lower():
            balcony = True
        parking = None
        if 'stellplatz' in title.lower() or 'stellplatz' in description.lower():
            parking = True
        terrace = None
        if 'terrasse' in title.lower() or 'terrasse' in description.lower():
            terrace = True
        washing_machine = None
        if 'waschmaschine' in description.lower():
            washing_machine = True
        elevator = None
        if 'aufzug' in description.lower():
            elevator = True

        energy_label = None
        try:
            energy_label = response.css('.energy td ::text').extract()
            energy_label = ' '.join(energy_label)
            if 'Energieeffizienzklasse' in energy_label:
                energy_label = energy_label.split('Energieeffizienzklasse: ')[1].split(' ')[0]
            else:
                energy_label = None
        except:
            pass

        images = response.css('.nb-maklerTool-expose-subPics a::attr(href)').extract()
        images = ['https://www.der-immo-tip.de/' + x for x in images]

        contact = response.css('.accContent div:nth-child(1) ::text').extract()
        landlord_name = contact[0]
        landlord_phone = contact[4]

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
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
