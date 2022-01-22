# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class MkiImmoSpider(scrapy.Spider):
    name = "mki_immo"
    start_urls = ['https://www.mki-immobilien.de/angebotstyp/vermietung/?proptype']
    allowed_domains = ["mki-immobilien.de"]
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
        objects = response.css('.propbox ')
        for object in objects:
            try:
                rented = object.css('.propthumb .newobj ::text')[0].extract()
                if 'reserviert' in rented.lower() or 'vermietet' in rented.lower():
                    continue
            except:
                pass
            property_url = object.css('a::attr(href)')[0].extract()
            title = object.css('.prop-all-left h2 ::text')[0].extract()
            if 'büro' in title.lower() or 'buro' in title.lower():
                continue
            elif 'haus' in title.lower():
                property_type = 'house'
            else:
                property_type = 'apartment'
            square_meters = None
            try:
                square_meters = object.css('.prop-size ::text').extract()
                square_meters = ''.join(square_meters)
                square_meters = remove_white_spaces(square_meters)
                square_meters = square_meters[:-2].strip()
                square_meters = int(''.join(x for x in square_meters if x.isdigit()))
            except:
                pass
            try:
                rent = object.css('.prop-price ::text')[0].extract()
                rent = remove_white_spaces(rent)
                rent = rent.split(' EUR')[0]
                if 'm²' in rent:
                    rent = rent.split(' ')[0]
                    rent = rent.replace(',', '.')
                    rent = float(rent)
                    rent = int(rent * square_meters)
                else:
                    if ',' in rent:
                        rent = rent.split(',')[0]
                    rent = int(''.join(x for x in rent if x.isdigit()))
            except:
                continue

            room_count = None
            try:
                room_count = object.css('.prop-rooms ::text').extract()
                room_count = ''.join(room_count)
                room_count = remove_white_spaces(room_count)
                room_count = room_count[0]
            except:
                pass
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'title': title, 'rent': rent, 'property_type': property_type, 'square_meters': square_meters, 'room_count': room_count})
        try:
            next_page = response.css('.next ::attr(href)')[0].extract()
            yield Request(url=next_page, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta['rent']
        property_type = response.meta['property_type']
        title = response.meta['title']
        square_meters = response.meta['square_meters']
        room_count = response.meta['room_count']

        description = response.css('p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        try:
            latlng = response.css('script:contains("map")::text').get()
            latitude = latlng.split(' {lat: ')[1].split(',')[0]
            longitude = latlng.split(', lng: ')[1].split('},')[0]
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        except:
            address = response.css('.propfacts a+a::text')[0].extract()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            longitude = str(longitude)
            latitude = str(latitude)
        list = response.css('td ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        floor = None
        if 'Etage:' in list:
            floor = list.split('Etage: ')[1].split(' ')[0]
        energy_label = None
        if 'Energieeffizienzklasse:' in list:
            energy_label = list.split('Energieeffizienzklasse: ')[1].split(' ')[0]

        bathroom_count = 1
        if 'Badezimmer:' in list:
            bathroom_count = list.split('Badezimmer: ')[1].split(' ')[0]
            bathroom_count = int(bathroom_count)

        utilities = None
        if 'Nebenkosten:' in list:
            utilities = list.split('Nebenkosten: ')[1].split('EUR')[0]
            if ',' in utilities:
                utilities = utilities.split(',')[0]
            utilities = int(utilities)
        deposit = None
        if 'Kaution:' in list:
            deposit = list.split('Kaution: ')[1].split(' ')[0]
            deposit = int(deposit[0])
            deposit = deposit * rent

        available_date = None
        if 'Verfügbar ab:' in list:
            available_date = list.split('Verfügbar ab: ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None
        parking = None
        if 'stellplatz' in description.lower():
            parking = True
        balcony = None
        if 'balkon' in description.lower():
            balcony = True
        washing_machine = None
        if 'waschmach' in description.lower():
            washing_machine = True

        images = response.css('.thickbox img::attr(src)').extract()
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
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
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
        item_loader.add_value("landlord_name", 'MKI Immobilien') # String
        item_loader.add_value("landlord_phone", '06181 26260') # String
        item_loader.add_value("landlord_email", 'info@mki-immobilien.de') # String

        self.position += 1
        yield item_loader.load_item()
