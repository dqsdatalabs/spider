# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup

class ImmoWeidlichSpider(scrapy.Spider):
    name = "immo_weidlich"
    start_urls = ['https://immobilien-weidlich.de/angebote.html?suche=&category=24&gr%C3%B6%C3%9Fe=0%2C10000']
    allowed_domains = ["immobilien-weidlich.de"]
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
        objects = response.css('.entry')
        for object in objects:
            try:
                rent = object.css('.price ::text')[0].extract()
                rent = int(''.join(x for x in rent if x.isdigit()))
            except:
                continue
            property_url = object.css(' a::attr(href)')[0].extract()
            title = object.css('.cc_immorealty_top a::text')[0].extract()
            square_meters = object.css(' li:nth-child(1)::text')[0].extract()
            try:
                square_meters = square_meters.split('m²')[0].strip()
                square_meters = int(''.join(x for x in square_meters if x.isdigit()))
            except:
                square_meters = None
            room_count = object.css(' li:nth-child(4)::text')[0].extract()
            bathroom_count = object.css(' li:nth-child(3)::text')[0].extract()
            try:
                room_count = int(room_count[0])
            except:
                room_count = 1
            try:
                bathroom_count = int(bathroom_count[0])
            except:
                bathroom_count = 1
            address = object.css('.place::text')[0].extract()
            property_type = address.split(',')[1].strip()
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'
            elif 'haus' in property_type.lower():
                property_type = 'house'
            address = address.split(',')[0].strip()
            yield Request(url=property_url, callback=self.populate_item, meta={'rent': rent, 'title': title, 'room_count': room_count, 'bathroom_count': bathroom_count, 'square_meters': square_meters, 'address': address, 'property_type': property_type})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta['title']
        rent = response.meta['rent']
        room_count = response.meta['room_count']
        bathroom_count = response.meta['bathroom_count']
        square_meters = response.meta['square_meters']
        address = response.meta['address']
        property_type = response.meta['property_type']

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        external_id = response.css('#main li:nth-child(1) ::text')[0].extract()
        external_id = external_id.split('Objektnummer:')[1].split()
        description = response.css('.property-description ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        energy_label = response.css('.textarea ::text').extract()
        energy_label = ' '.join(energy_label)
        energy_label = remove_white_spaces(energy_label)
        if 'Energieeffizienzklasse' in energy_label:
            energy_label = energy_label.split('Energieeffizienzklasse: ')[1].split(' ')[0]
        else:
            energy_label = None

        list = response.css('.myprice ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        utilities = None
        if 'Nebenkosten: € ' in list:
            utilities = int(list.split('Nebenkosten: € ')[1].split(',')[0])
        heating_cost = None
        if 'Heizkosten: € ' in list:
            heating_cost = int(list.split('Heizkosten: € ')[1].split(',')[0])

        parking = None
        if 'stellplatz' in description.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower():
            dishwasher = True
        terrace = None
        if 'terrase' in description.lower():
            terrace = True
        elevator = None
        if 'aufzug' in description.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower():
            balcony = True

        if 'Stellplatz' in list:
            parking = True

        images = response.css('.property-gallery .image_container img::attr(src)').extract()
        images = ['https://immobilien-weidlich.de/' + x for x in images]

        if rent <= 0 and rent > 40000:
            return

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
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Immobilien Jürgen Weidlich GmbH') # String
        item_loader.add_value("landlord_phone", '+49 8091 30 10') # String
        item_loader.add_value("landlord_email", 'info@immobilien-weidlich.de') # String

        self.position += 1
        yield item_loader.load_item()
