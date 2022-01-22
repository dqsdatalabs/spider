# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class ApakusMaklerSpider(scrapy.Spider):
    name = "apakus_makler"
    # start_urls = ['https://www.abakus-makler.de/vermietung-leipzig.html?pg=2']
    start_urls = ['https://www.abakus-makler.de/wohnungen-leipzig.html']
    allowed_domains = ["abakus-makler.de"]
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
        objects = response.css('.resultlist-item-content')
        for object in objects:
            property_url = object.css('.resultlist-item-content-pic a::attr(href)')[0].extract()
            external_id = property_url.replace("ID", "")
            property_url = 'https://www.abakus-makler.de/' + property_url
            listp = object.css('.resultlist-details-table td::text').extract()
            listp = ' '.join(listp)
            listp = remove_white_spaces(listp)

            if 'Miete: ' in listp:
                rent = listp.split('Miete: ')[1].split(',')[0]
                rent = int(''.join(x for x in rent if x.isdigit()))
            else:
                continue
            room_count = 1
            if 'Zimmer: ' in listp:
                room_count = listp.split('Zimmer: ')[1].split(' ')[0]
                if '.' in room_count:
                    room_count = int(room_count[0]) + 1
            square_meters = None
            if 'Wohnfläche: ' in listp:
                square_meters = listp.split('Wohnfläche: ')[1].split(',')[0]
            elevator = None
            if 'lift' in listp.lower():
                elevator = True
            balcony = None
            if 'balkon' in listp.lower():
                balcony = True
            parking = None
            if 'garage' in listp.lower():
                parking = True
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'room_count': room_count, 'square_meters': square_meters,
                                'external_id': external_id, 'balcony': balcony, 'elevator': elevator, 'parking': parking})
        last_page = response.css('td:nth-last-child(1) a::attr(href)')[1].extract()
        last_page = 'https://www.abakus-makler.de' + last_page
        current_page = response.url
        page_next = response.css('.pages+ td a::attr(href)')[0].extract()
        if current_page != last_page:
            yield Request(url='https://www.abakus-makler.de' + page_next, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.meta['external_id']
        rent = response.meta['rent']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        balcony = response.meta['balcony']
        elevator = response.meta['elevator']
        parking = response.meta['parking']

        title = response.css('#expose-headline ::text')[0].extract()

        description = response.css('.exposeFreetextBody ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        list = response.css('#exposeDetailsTable td ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        floor = None
        if 'etage: ' in description:
            floor = description.split('etage: ')[1].split(' ')[0]
        address = None
        if 'Adresse: ' in list:
            address = list.split('Adresse: ')[1].split('Zimmer')[0]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        available_date = None
        if 'Verfügbar ab: ' in list:
            available_date = list.split('Verfügbar ab: ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None
        deposit = None
        if 'Kaution: € ' in list:
            deposit = list.split('Kaution: € ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        if 'Nebenkosten: € ' in list:
            utilities = list.split('Nebenkosten: € ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))

        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower():
            dishwasher = True
        terrace = None
        if 'terras' in description.lower() or 'terras' in list.lower():
            terrace = True
        if 'aufzug' in description.lower() or 'aufzug' in list.lower():
            elevator = True
        if 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

        images = response.css('.galleryItem ::attr(href)').extract()
        images = ['https://www.abakus-makler.de/' + x for x in images]

        if int(rent) <= 0 and int(rent) > 40000:
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", 1) # Int

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

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Abakus Immobilien') # String
        item_loader.add_value("landlord_phone", '0341 230 77 77') # String
        item_loader.add_value("landlord_email", 'info@abakus-makler.de') # String

        self.position += 1
        yield item_loader.load_item()
