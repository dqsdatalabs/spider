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


class RfischerImmoSpider(scrapy.Spider):
    name = "rfischer_immo"
    start_urls = ['https://www.rfischer-immobilien.de/immobilien/immobilien-zur-miete.php']
    allowed_domains = ["rfischer-immobilien.de"]
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
        objects = response.css('.immo_details')
        for object in objects:
            property_url = object.css(' a::attr(href)')[0].extract()
            title = object.css('.immo_objectHeadline ::text')[0].extract()
            rented = object.css('.lowAmount mark ::text').extract()
            if 'Vermietet' in rented:
                continue
            listp = object.css('td ::text').extract()
            listp = ' '.join(listp)
            listp = remove_white_spaces(listp)
            if 'Kaltmiete ' in listp:
                rent = listp.split('Kaltmiete ')[1].split(',')[0]
            elif 'Warmmiete ' in listp:
                rent = listp.split('Warmmiete ')[1].split(',')[0]
            else:
                continue
            rent = int(''.join(x for x in rent if x.isdigit()))
            external_id = None
            if 'Objekt-Nr. ' in listp:
                external_id = listp.split('Objekt-Nr. ')[1].split(' ')[0]
            address = None
            if 'PLZ / Ort ' in listp:
                address = listp.split('PLZ / Ort ')[1].split(' ')[0]
            square_meters = None
            if 'fläche ' in listp:
                square_meters = listp.split('fläche ')[1].split('m')[0]
                if ',' in square_meters:
                    square_meters = listp.split(',')[0]

            room_count = 1
            if 'Zimmer ' in listp:
                room_count = listp.split('Zimmer ')[1].split(' ')[0]
                if ',' in room_count:
                    room_count = int(listp.split(',')[0]) + 1

            yield Request(url=property_url, callback=self.populate_item,
                          meta={'rent': rent, 'title': title, 'room_count': room_count, 'square_meters': square_meters,
                                'address': address, 'external_id': external_id})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta['rent']
        title = response.meta['title']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        external_id = response.meta['external_id']

        title = response.css('.immo_headlineDetail ::text')[0].extract()
        description = response.css('.row:nth-child(6) .col ::text, .row:nth-child(4) .col ::text, .row:nth-child(5) .col::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        images = response.css('.swiper-slide a::attr(href)').extract()

        list = response.css('dd ::text, dt ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        if 'Objektart ' in list:
            property_type = list.split('Objektart ')[1].split(' ')[0]
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
        else:
            return

        bathroom_count = 1
        if 'Badezimmer ' in list:
            bathroom_count = list.split('Badezimmer ')[1].split(' ')[0]
        address = None
        if 'Adresse der Immobilie ' in list:
            address = list.split('Adresse der Immobilie ')[1].split('Objektart ')[0]
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, trash = extract_location_from_coordinates(longitude, latitude)
            longitude = str(longitude)
            latitude = str(latitude)
        deposit = None
        if 'Kaution ' in list:
            deposit = list.split('Kaution ')[1].split(' ')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
            deposit = rent * deposit
        utilities = None
        if 'Nebenkosten ' in list:
            utilities = list.split('Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        energy_label = None
        if 'Energieeffizienzklasse ' in list:
            energy_label = list.split('Energieeffizienzklasse ')[1].split(' ')[0]
        elif 'Energiekennwert ' in list:
            energy_label = list.split('Energiekennwert ')[1].split(' ')[0]
            if ',' in energy_label:
                energy_label = energy_label.split(',')[0]
                energy_label = int(energy_label)
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

        available_date = None
        if 'Verfügbar ab ' in list:
            available_date = list.split('Verfügbar ab ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

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

        info = response.css('.immo_fastInfos dd ::text').extract()
        landlord_name = info[1]
        landlord_number = info[-1]
        landlord_email = info[2] + '@' + info[4]

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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
