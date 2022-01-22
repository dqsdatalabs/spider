# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class ImmoBernhardtSpider(scrapy.Spider):
    name = "immo_bernhardt"
    start_urls = ['https://www.immobilienbernhardt.de/immo-miete.xhtml?f[20083-9]=miete']
    allowed_domains = ["immobilienbernhardt.de"]
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
        objects = response.css('.list-object')
        for object in objects:
            property_url = object.css('.image a::attr(href)')[0].extract()
            property_url = 'https://www.immobilienbernhardt.de/' + property_url
            rented = object.css('.image  a > ul > li ')[0].extract()
            if 'reserved' in rented.lower() or 'rented' in rented.lower():
                continue
            square_meters = object.css('.details span span ::text')[0].extract()
            room_count = 1
            try:
                room_count = object.css('.details span span ::text')[1].extract()
            except:
                pass
            city = object.css('.city ::text')[0].extract()
            rent = object.css('p span span::text')[0].extract()
            rent = rent[:-2].strip()
            rent = int(''.join(x for x in rent if x.isdigit()))
            yield Request(url=property_url, callback=self.populate_item, meta={'rent': rent, 'city': city, 'room_count': room_count, 'square_meters': square_meters})
        last_page = response.css('span~ a+ a::attr(href)')[0].extract()
        current_page = response.url
        page_next = response.css('span+ a::attr(href)')[0].extract()
        if current_page != last_page:
            yield Request(url='https://www.immobilienbernhardt.de/' + page_next, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta["rent"]
        city = response.meta["city"]
        room_count = response.meta["room_count"]
        square_meters = response.meta["square_meters"]

        title = response.css('.detail h2 ::text')[0].extract()
        title = remove_white_spaces(title)
        description = response.css('span:nth-child(2) span  ::text, .information span:nth-child(1) span ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        energy_label = None
        try:
            if 'kwh' in description.lower():
                energy_label = description.lower().split('kwh')[0].split('darf: ')[1]
                energy_label = int(energy_label.split('.')[0])
                if energy_label >= 250:
                    energy_label = 'H'
                elif energy_label >= 200 and energy_label <= 250:
                    energy_label = 'G'
                elif energy_label >= 160 and energy_label <= 200:
                    energy_label = 'F'
                elif energy_label >= 125 and energy_label <= 160:
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
        except:
            pass

        address = response.css('script:contains("estate")::text').get()
        address = remove_white_spaces(address)
        latitude = address.split('"lat":')[1].split("',")[0]
        latitude = latitude[2:]
        longitude = address.split('"lng":')[1].split("' ")[0]
        longitude = longitude[2:]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        list = response.css('.details-desktop td ::text').extract()
        list = ' '.join(list)
        property_type = 'apartment'
        if 'Objekttyp' in list:
            property_type = list.split('Objekttyp ')[1].split(' ')[0]
        if 'zimmer' in property_type.lower():
            property_type = 'room'
        else:
            property_type = 'apartment'
        deposit = None
        if 'Kaution' in list:
            deposit = list.split('Kaution ')[1].split(' €')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten ')[1].split(' €')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        external_id = None
        if 'Objnr' in list:
            external_id = list.split('Objnr ')[1].split(' ')[0]
        zipcode = None
        if 'PLZ' in list:
            zipcode = list.split('PLZ ')[1].split(' ')[0]
        floor = None
        if ' Etage ' in list:
            floor = list.split(' Etage ')[1].split(' ')[0]
        elevator = None
        if 'aufzug' in list.lower():
            elevator = True
        parking = None
        if 'stellplatz' in list.lower():
            parking = True
        balcony = None
        if 'balkon' in list.lower() or 'balkon' in description.lower():
            balcony = True
        washing_machine = None
        if 'waschmasc' in description.lower():
            washing_machine = True
        furnished = None
        if 'möbliertes' in title.lower():
            furnished = True

        bathroom_count = 1
        if 'Badezimmer' in list:
            try:
                bathroom_count = list.split('Badezimmer ')[1].split(' ')[0]
            except:
                pass

        images = response.css('.gallery ::attr(data-img)').extract()


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

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("landlord_name", 'Immobilien Bernhardt e.K') # String
        item_loader.add_value("landlord_phone", '0841483861') # String
        item_loader.add_value("landlord_email", 'info@immobilienbernhardt.de') # String

        self.position += 1
        yield item_loader.load_item()
