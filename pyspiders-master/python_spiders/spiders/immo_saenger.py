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


class ImmoSaengerSpider(scrapy.Spider):
    name = "immo_saenger"
    start_urls = ['https://immobilien-saenger.de/mieten/']
    allowed_domains = ["immobilien-saenger.de"]
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
        objects = response.css('.oo-listobject')
        for object in objects:
            property_url = object.css(' .oo-detailslink a::attr(href)')[0].extract()
            rented = object.css(' .oo-listimage span::text')[0].extract()
            if 'vermietet' in rented:
                continue
            title = object.css(' .oo-listtitle::text')[0].extract()
            title = remove_white_spaces(title)
            if 'stellplatz' in title.lower():
                continue
            listp = object.css(' .oo-listtd::text').extract()
            listp = ' '.join(listp)
            listp = remove_white_spaces(listp)
            if 'Kaltmiete ' in listp:
                rent = listp.split('Kaltmiete ')[1].split(',')[0]
                rent = int(''.join(x for x in rent if x.isdigit()))
            else:
                continue
            room_count = None
            if 'Zimmer ' in listp:
                room_count = listp.split('Zimmer ')[1].split(' ')[0]
            if ',' in room_count:
                room_count = int(room_count.split(',')[0]) + 1
            square_meters = None
            if 'Wohnfläche ca. ' in listp:
                square_meters = listp.split('Wohnfläche ca. ')[1].split(' ')[0]
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'room_count': room_count, 'title': title, 'square_meters': square_meters})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta['rent']
        title = response.meta['title']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']

        description = response.css('.oo-detailsfreetext ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        list = response.css('.oo-detailslisttd ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)
        deposit = None
        if 'Kaution ' in list:
            deposit = list.split('Kaution ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        if '€ Nebenkosten ' in list:
            utilities = list.split('€ Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        external_id = None
        if 'ImmoNr ' in list:
            external_id = list.split('ImmoNr ')[1].split(' ')[0]
        floor = None
        if 'Etage ' in list:
            floor = list.split('Etage ')[1].split(' ')[0]
        energy_label = None
        if 'Energieeffizienzklasse ' in list:
            energy_label = list.split('Energieeffizienzklasse ')[1].split(' ')[0]
        elif 'Endenergiebedarf ' in list:
            energy_label = int(list.split('Endenergiebedarf ')[1].split(' ')[0])
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

        bathroom_count = 1
        if 'Badezimmer ' in list:
            bathroom_count = list.split('Badezimmer ')[1].split(' ')[0]

        location = None
        if 'Ort ' in list:
            location = list.split('Ort ')[1].split(' ')[0]
        street = None
        if ' Straße ' in list:
            street = list.split(' Straße ')[1].split(' ')[0]
        zipcode = None
        if 'PLZ ' in list:
            zipcode = list.split('PLZ ')[1].split(' ')[0]
        city = None
        longitude = None
        latitude = None
        address = None
        try:
            address = street + ', ' + zipcode + ', ' + location
            longitude, latitude = extract_location_from_address(address)
            zz, city, aa = extract_location_from_coordinates(longitude, latitude)
            longitude = str(longitude)
            latitude = str(latitude)
        except:
            pass
        parking = None
        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower():
            dishwasher = True
        terrace = None
        if 'terras' in description.lower() or 'terrase ja' in list.lower():
            terrace = True
        elevator = None
        if 'fahrstuhl' in description.lower() or 'fahrstuhl' in list.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower() or 'balkon ja' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

        images = response.css('.oo-detailspicture::attr(style)').extract()
        images = [image.split("background-image: url('")[1].split("');")[0] for image in images]

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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Angelika Sänger Immobilien') # String
        item_loader.add_value("landlord_phone", '+49 (0) 2173 16 50 696') # String
        item_loader.add_value("landlord_email", 'info@immobilien-saenger.de') # String

        self.position += 1
        yield item_loader.load_item()
