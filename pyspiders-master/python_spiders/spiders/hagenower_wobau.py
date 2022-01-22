# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class HagenowerWobauSpider(scrapy.Spider):
    name = "hagenower_wobau"
    start_urls = ['https://hagenower-wobau.de/fuer-interessenten/mieten/']
    allowed_domains = ["hagenower-wobau.de"]
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
        objects = response.css('.box-facility-data')
        for object in objects:
            property_url = object.css('h3 a::attr(href)')[0].extract()
            property_url = 'https://hagenower-wobau.de/' + property_url
            title = object.css('h3 a::text')[0].extract()
            square_meters = object.css('.databox-2 h3::text')[0].extract()
            room_count = object.css('.databox-3 h3::text')[0].extract()
            address = object.css('h3+ p::text')[0].extract()
            prelist = object.css('.box-facility-databox+ p ::text').extract()
            prelist = ' '.join(prelist)
            square_meters = int(square_meters.split(',')[0])
            yield Request(url=property_url, callback=self.populate_item, meta={'title': title, 'room_count': room_count, 'square_meters': square_meters, 'address': address, 'prelist': prelist})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta['title']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        address = response.meta['address']
        prelist = response.meta['prelist']
        address = address.split('(')[0].strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        elevator = None
        if 'fahrstuhl' in prelist.lower():
            elevator = True
        balcony = None
        if 'balkon' in prelist.lower():
            balcony = True
        furnished = None
        if 'balkon' in prelist.lower():
            balcony = True
        floor = None
        if 'Etage:' in prelist:
            floor = prelist.split('Etage: ')[1] .split()

        if 'wohnung' in title.lower():
            property_type = 'apartment'

        description = response.css('.et_pb_code_4 p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

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
        if 'fahrst' in description.lower():
            elevator = True
        if 'balkon' in description.lower():
            balcony = True

        list = response.css('.et_pb_code_inner td ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        if 'Kaltmiete:' in list:
            rent = int(list.split('Kaltmiete: ')[1].split(',')[0])
        else:
            return
        utilities = None
        if 'Nebenkosten:' in list:
            utilities = int(list.split('Nebenkosten: ')[1].split(',')[0])
        deposit = None
        if 'Kaution:' in list:
            deposit = list.split('Kaution: ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        energy_label = None
        if 'Energieeffizienzklasse:' in list:
            energy_label = list.split('Energieeffizienzklasse: ')[1]
        available_date = None
        if 'Frei ab:' in list:
            available_date = list.split('Frei ab: ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

        landlord_name = response.css('.et_pb_code_inner h3 ::text')[0].extract()
        landlord_number = response.css('.txt-fon ::text')[0].extract()
        landlord_email = response.css('#et-boc .txt-mail a ::text')[0].extract()

        bathroom_count = 1

        images = response.css('.portrait img ::attr(src)').extract()
        images = ['https://hagenower-wobau.de' + x for x in images]

        if rent <= 0 and rent > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
