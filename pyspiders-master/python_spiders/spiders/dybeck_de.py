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


class DybeckDeSpider(scrapy.Spider):
    name = "dybeck_de"
    start_urls = ['https://www.dybeck.de/angebote/?mt=rent&address&sort=date%7Cdesc']
    allowed_domains = ["dybeck.de"]
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
        objects = response.css('.my-1')
        for object in objects:
            property_url = object.css(' a::attr(href)')[0].extract()
            rented = object.css('.immolisting__banner::text').extract()
            if 'vermietet' in rented:
                continue
            content = object.css('.immolisting__content ::text').extract()
            title = content[1]
            if 'wohnung' in title.lower():
                property_type = 'apartment'
            elif 'haus' in title.lower():
                property_type = 'house'
            else:
                continue
            rent = content[5]
            room_count = content[13]
            square_meters = content[9].split('m')[0]
            if ',' in square_meters:
                square_meters = int(square_meters.split(',')[0]) + 1
            try:
                rent = int(''.join(x for x in rent if x.isdigit()))
            except:
                continue
            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'title': title, 'room_count': room_count, 'square_meters': square_meters,
                                'property_type': property_type})
        try:
            page_next = response.css('.next::attr(href)')[0].extract()
            yield Request(url=page_next, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta['rent']
        title = response.meta['title']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        property_type = response.meta['property_type']

        description = response.css('.immo-single__tabs-content ::attr(data-tabs)')[0].extract()
        description = description.split('"rawValue":"')[1].split('","value":')[0]
        description = description_cleaner(description)

        list = response.css('.immo-single__contact-person .wp-block-column+ .wp-block-column div ::text, .immo-single__contact-person li ::text, .epass__info-list li ::text, .col-sm-8 ::text, .col-sm-16 ::text, .bool-fields li ::text, .key ::text, .col-sm-16 .value::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        address = None
        if 'Standort ' in list:
            address = list.split('Standort ')[1].split(' Preise ')[0]
        deposit = None
        if 'Kaution ' in list:
            deposit = list.split('Kaution ')[1].split(' ')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        heating_cost = None
        if 'enthalten Heizkosten ' in list:
            heating_cost = list.split('enthalten Heizkosten ')[1].split(' ')[0]
            heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
        utilities = None
        if 'Nebenkosten ' in list:
            utilities = list.split('Nebenkosten ')[1].split(' ')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        external_id = None
        if 'Objekt-Nr ' in list:
            external_id = list.split('Objekt-Nr ')[1].split(' ')[0]
        floor = None
        if 'Etage ' in list:
            floor = list.split('Etage ')[1].split(' ')[0]
        bathroom_count = 1
        if 'Badezimmer ' in list:
            bathroom_count = list.split('Badezimmer ')[1].split(' ')[0]
        landlord_number = None
        if 'Telefon: ' in list:
            landlord_number = list.split('Telefon: ')[1].split(' Telefax:')[0]
        landlord_name = None
        if 'Ansprechpartner/in ' in list:
            landlord_name = list.split('Ansprechpartner/in ')[1].split(' Telefon:')[0]

        energy_label = None
        if 'Energieeffizienzklasse ' in list:
            energy_label = list.split('Energieeffizienzklasse ')[1].split(' ')[0]
        elif 'Endenergieverbrauch ' in list:
            energy_label = int(list.split('Endenergieverbrauch ')[1].split(' ')[0])
            if energy_label >= 250:
                energy_label = 'H'
            elif energy_label >= 225 and energy_label <= 250:
                energy_label = 'G'
            elif energy_label >= 150 and energy_label <= 175:
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

        images = response.css('.swiper-lazy ::attr(data-background)').extract()

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

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
        if 'aufzug' in description.lower() or 'aufzug' in list.lower() or 'fahrstuhl' in description.lower() or 'fahrstuhl' in list.lower():
            elevator = True
        balcony = None
        if 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

        if int(rent) <= 0 and int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
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
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", 'mailto:info@dybeck.de') # String

        self.position += 1
        yield item_loader.load_item()
