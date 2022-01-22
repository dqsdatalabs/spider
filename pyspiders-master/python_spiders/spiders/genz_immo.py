# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class GenzImmoSpider(scrapy.Spider):
    name = "genz_immo"
    start_urls = ['https://www.genz-immobilien.de/vermietung/']
    allowed_domains = ["genz-immobilien.de"]
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
        objects = response.css('.arc_3 a')
        for object in objects:
            property_url = object.css('::attr(href)')[0].extract()
            property_url = 'https://www.genz-immobilien.de/' + property_url
            title = object.css('h2::text')[0].extract()
            if 'arbeitsraum' in title.lower():
                continue
            rent = object.css('strong::text')[0].extract()
            try:
                rent = rent.split(',')[0]
                rent = int(''.join(x for x in rent if x.isdigit()))
            except:
                continue
            prelist = object.css('p:nth-child(1)::text').extract()
            prelist = ' '.join(prelist)
            prelist = remove_white_spaces(prelist)
            square_meters = None
            if 'Wohnfläche ca.' in prelist:
                square_meters = prelist.split('Wohnfläche ca. ')[1].split(',')[0]
            room_count = 1
            if 'Zimmer' in prelist:
                room_count = prelist.split(' Zimmer')[0].split('m²')[1].strip()
                if ',' in room_count:
                    room_count = int(room_count[0]) + 1
                else:
                    room_count = int(room_count[0])
            terrace = None
            if 'terrase' in prelist.lower():
                terrace = True
            balcony = None
            if 'balkon' in prelist.lower():
                balcony = True
            yield Request(url=property_url, callback=self.populate_item, meta={'rent': rent, 'title': title, 'terrace': terrace, 'room_count': room_count, 'square_meters': square_meters, 'balcony': balcony})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta['rent']
        title = response.meta['title']
        terrace = response.meta['terrace']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        balcony = response.meta['balcony']

        if square_meters == None:
            try:
                square_meters = response.css('.immobilien-preis+ .block p ::text').extract()
                square_meters = ' '.join(square_meters)
                square_meters = square_meters.split('ca. ')[1].split(',')[0]
            except:
                pass
        description = response.css('.immobilien-beschreibung2 p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        address = None
        if 'nähe von ' in title:
            address = title.split('nähe von ')[1]
        if 'n in' in title:
            address = title.split('n in ')[1]
        if 'i in ' in title:
            address = title.split('i in ')[1].split(',')[0]

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)


        energy_label = None
        if 'energieeffizienzklasse:' in description:
            energy_label = description.split('energieeffizienzklasse: ')[1].split(' ')[0]

        list1 = response.css('.immobilien-preis+ .block p::text').extract()
        list1 = ' '.join(list1)

        if 'balkon' in list1.lower():
            balcony = True
        parking = None
        if 'garage' in list1.lower():
            parking = True

        list = response.css('.immobilien-preis p::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        deposit = None
        if 'Mietkaution: ' in list:
            deposit = int(list.split('Mietkaution: ')[1].split(' ')[0])
            deposit = deposit * rent
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split(',00 Nebenkosten ')[0].split('EUR ')[1]
            utilities = int(''.join(x for x in utilities if x.isdigit()))

        images = response.css('.caroufredsel_gallery img::attr(src)').extract()
        images = ['https://www.genz-immobilien.de/' + x for x in images]

        list2 = response.css('p:nth-child(5) strong::text').extract()
        list2 = ' '.join(list2)
        list2 = remove_white_spaces(list2)

        if 'haus' in list2.lower():
            property_type = 'house'
        else:
            property_type = 'apartment'
        available_date = None
        if 'Ihnen ab dem' in list2:
            available_date = list2.split('Ihnen ab dem ')[1].split(' zur')[0]
        try:
            import datetime
            import dateparser
            available_date = available_date.strip()
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        except:
            available_date = None


        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower():
            dishwasher = True
        if 'terras' in description.lower() or 'terras' in list1.lower() or 'terras' in title.lower():
            terrace = True
        elevator = None
        if 'aufzug' in description.lower() or 'aufzug' in list.lower():
            elevator = True

        if 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

        bathroom_count = 1


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
        #item_loader.add_value("floor", floor) # String
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
        item_loader.add_value("landlord_name", 'Hans-Heinrich Genz, Carsten Genz') # String
        item_loader.add_value("landlord_phone", '04165 6933') # String
        item_loader.add_value("landlord_email", 'mail@genz-immobilien.de') # String

        self.position += 1
        yield item_loader.load_item()
