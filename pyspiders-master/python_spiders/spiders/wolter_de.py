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


class WolterDeSpider(scrapy.Spider):
    name = "wolter_de"
    start_urls = ['https://www.wolter.de/aktuelle-immobilien/mietwohnungen/']
    allowed_domains = ["wolter.de"]
    country = 'Germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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
        objects = response.css('.wolter-immo .wolter-immo-list-object')
        for object in objects:
            property_url = object.css(' h5 a::attr(href)').extract()
            if property_url == []:
                continue
            property_url = 'https://www.wolter.de/aktuelle-immobilien/mietwohnungen/' + property_url[0]
            title = object.css(' h5 a::text')[0].extract()
            listp = object.css('td::text').extract()
            listp = ' '.join(listp)
            listp = remove_white_spaces(listp)
            if 'Preis: ' in listp:
                rent = listp.split('Preis: ')[1].split(' ')[0]
                rent = int(''.join(x for x in rent if x.isdigit()))
            else:
                continue
            address = None
            if 'Ort: ' in listp:
                address = listp.split('Ort: ')[1].split(' Art')[0]
            square_meters = None
            if 'Fläche: ' in listp:
                square_meters = listp.split('Fläche: ')[1].split(' ')[0]
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'address': address, 'title': title, 'square_meters': square_meters})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta['rent']
        title = response.meta['title']
        address = response.meta['address']
        square_meters = response.meta['square_meters']

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        list = response.css('td ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)
        deposit = None
        if 'Kaution:&nbsp ' in list:
            deposit = list.split('Kaution:&nbsp ')[1].split(' ')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        if 'Nebenkosten:&nbsp ' in list:
            utilities = list.split('Nebenkosten:&nbsp ')[1].split(' ')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        heating_cost = None
        if 'Mtl. Heizkosten:&nbsp ' in list:
            heating_cost = list.split('Mtl. Heizkosten:&nbsp ')[1].split(' ')[0]
            heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
        floor = None
        if 'Stock:&nbsp ' in list:
            floor = list.split('Stock:&nbsp ')[1].split(' ')[0]
        room_count = None
        if 'Zimmer/Räume:&nbsp ' in list:
            room_count = list.split('Zimmer/Räume:&nbsp ')[1].split(' ')[0]
        energy_label = None
        if 'Energieeffizienzklasse:&nbsp ' in list:
            energy_label = list.split('Energieeffizienzklasse:&nbsp ')[1].split(' ')[0]
        desc1 = ' '
        if 'Objektbeschreibung: ' in list:
            desc1 = list.split('Objektbeschreibung: ')[1].split(' Ausstattung:')[0]
        desc2 = ' '
        if 'Ausstattung: ' in list:
            desc2 = list.split('Ausstattung: ')[1].split(' Lage:')[0]
        desc3 = ' '
        if 'Lage: ' in list:
            desc3 = list.split('Lage: ')[1].split(' Sonstiges:')[0]
        description = desc1+desc2+desc3
        description = remove_white_spaces(description)

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
        if 'terras' in description.lower() or 'terras' in list.lower():
            terrace = True
        elevator = None
        if 'aufzug' in description.lower() or 'aufzug' in list.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

        floor_plan_images = None
        images = response.css('.attachment-full img::attr(src), .img-fluid ::attr(src)').extract()
        for image in images:
            if 'reserviert' in image:
                return
            elif 'grundriss' in image:
                floor_plan_images = [image]

        if int(rent) <= 0 and int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
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

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'JO. WOLTER IMMOBILIEN') # String
        item_loader.add_value("landlord_phone", '0531 - 244770') # String
        item_loader.add_value("landlord_email", 'jo@wolter.de') # String

        self.position += 1
        yield item_loader.load_item()
