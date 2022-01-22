# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import json
from scrapy.http.request.json_request import JsonRequest

class InfocasaImmoSpider(scrapy.Spider):
    name = "infocasa_immo"
    start_urls = ['http://infocasaimmobiliare.it/it/Affitti/']
    country = 'Italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(
            url='http://infocasaimmobiliare.it/it/Affitti/',
            callback=self.parse,
            body='',
            method='GET',
            headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_urls = response.css('li a::attr(href)').extract()
        property_urls = ['http://infocasaimmobiliare.it' + x for x in property_urls]
        for property_url in property_urls:
            if len(property_url) > 20 and 'it/Affitti/' in property_url:
                yield Request(url=property_url, callback=self.populate_item,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css("h1::text")[0].extract()
        if 'ufficio' in title.lower():
            return
        external_id = response.css(".active::text")[0].extract()
        external_id = external_id[7:].strip()
        description = response.css(".text-justify::text").extract()
        description = ' '.join(description)
        if 'appartamento' in description:
            property_type = 'apartment'
        if 'informazioni' in description:
            landlord_info = description.split('informazioni ')[1].split('Infocasa Immobiliare')[0]
            landlord_name = landlord_info.split('contattare ')[1].split(' al ')[0]
            if any(char.isdigit() for char in landlord_info):
                landlord_number = ''.join(x for x in landlord_info if x.isdigit())
            else:
                landlord_number = '3281805785'

        description = description.split(' Per ricevere ulteriori')[0]
        city = 'Bolango'

        furnished = None
        if 'arredato' in description:
            furnished = True
        balcony = None
        if 'balconi' in description:
            balcony = True
        parking = None
        if 'posti auto' in description or 'garage' in description:
            parking = True
        washing_machine = None
        if 'lavatrice' in description.lower():
            washing_machine = True
        dishwasher = None
        if 'lavastoviglie' in description:
            dishwasher = True

        try:
            rent = response.css("strong::text")[1].extract()
            if any(char.isdigit() for char in rent):
                rent = int(''.join(x for x in rent if x.isdigit()))
        except:
            return
        list = response.css(".list li::text").extract()
        list = ''.join(list)
        utilities = None
        try:
            utilities = list.split('Spese condominiali: ')[1].split(' €')[0]
            if any(char.isdigit() for char in utilities):
                utilities = int(''.join(x for x in utilities if x.isdigit()))
        except:
            pass
        square_meters = list.split('Mq: ')[1].split('Locali:')[0]
        square_meters = int(square_meters)
        room_count = list.split('Camere: ')[1].split('Bagni:')[0]
        room_count = int(room_count)
        bathroom_count = list.split('Bagni: ')[1].split('Posti auto:')[0]
        bathroom_count = int(bathroom_count)
        energy_label = list.split('Classe: ')[1].strip()
        images = response.css(".img-lighbox-thumbnails a::attr(href)").extract()
        images = ['http://infocasaimmobiliare.it' + x for x in images]


        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        #item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", 'info@infocasaimmobiliare.it') # String

        self.position += 1
        yield item_loader.load_item()
