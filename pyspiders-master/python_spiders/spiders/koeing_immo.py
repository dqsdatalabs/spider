# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import datetime
import dateparser

class KoeingImmoSpider(scrapy.Spider):
    name = "koeing_immo"
    start_urls = ['https://portal.immobilienscout24.de/ergebnisliste/72461764',
                  'https://portal.immobilienscout24.de/ergebnisliste/70458505',
                  'https://portal.immobilienscout24.de/ergebnisliste/19880865']
    allowed_domains = ["koenig-immo.com", "immobilienscout24.de"]
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
        property_urls = response.css('.result__list__element__infos--figcaption a::attr(href)').extract()
        if property_urls != []:
            property_urls = ['https://portal.immobilienscout24.de/' + x for x in property_urls]
        else:
            return
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list = response.css('li p::text').extract()
        list = ' '.join(list)

        if 'Kaufpreis' in list or 'Laden' in list:
            return
        description = response.css('.expose--text~ .expose--text+ .expose--text:nth-child(6) p ::text').extract()
        description = ' '.join(description)
        external_id = response.css('.form+ .expose--text p:nth-child(1)::text')[0].extract()
        external_id = external_id.split('Scout-Objekt-ID: ')[1]
        title = response.css('.is24__block__responsive--col1 h4::text')[0].extract()
        title = title.replace('*','').strip()
        address = response.css('.expose--text__address p::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        rent = None
        utilities = None
        if 'Kaltmiete:' in list:
            rent = list.split('Kaltmiete: ')[1].split(',')[0]
            utilities = list.split('Gesamtmiete: ')[1].split(',')[0]
            deposit = list.split('Kaution oder Genossenschaftsanteile: ')[1].split(',')[0]

        if any(char.isdigit() for char in deposit):
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        if utilities:
            if any(char.isdigit() for char in utilities):
                utilities = int(''.join(x for x in utilities if x.isdigit()))
            utilities = utilities-rent
        property_type = list.split('Wohnungstyp: ')[1].split(' ')[0]
        if 'wohnung' in property_type:
            property_type = 'apartment'
        square_meters = int(list.split('Wohnfläche ca.: ')[1].split(',')[0])
        room_count = list.split('Zimmer: ')[1].split('0')[0]
        if any(char.isdigit() for char in room_count):
            room_count = ''.join(x for x in room_count if x.isdigit())
        if len(room_count) > 1:
            room_count = int(room_count[0]) + 1
        else:
            room_count = int(room_count[0])
        bathroom_count = int(list.split('Badezimmer: ')[1].split(' ')[0])
        if 'Bezugsfrei ab' in list:
            available_date = list.split('Bezugsfrei ab: ')[1].split(' ')[0]
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        energy_label = None
        if 'Energieverbrauchs​kennwert' in list:
            energy_label = int(list.split('Energieverbrauchs​kennwert                : ')[1].split(',')[0])
        if 'Endenergiebedarf' in list:
            energy_label = int(list.split('Endenergiebedarf                : ')[1].split(',')[0])
        if 'Endenergieverbrauch' in list:
            energy_label = int(list.split('Endenergieverbrauch                : ')[1].split(',')[0])
        if energy_label != None:
            if energy_label >= 92:
                energy_label = 'A'
            elif energy_label >= 81 and energy_label <= 91:
                energy_label = 'B'
            elif energy_label >= 69 and energy_label <= 80:
                energy_label = 'C'
            elif energy_label >= 55 and energy_label <= 68:
                energy_label = 'D'
            elif energy_label >= 39 and energy_label <= 54:
                energy_label = 'E'
            elif energy_label >= 21 and energy_label <= 38:
                energy_label = 'F'
            elif energy_label >= 1 and energy_label <= 20:
                energy_label = 'G'

        floor = None
        pets_allowed = None
        if 'Etage' in list:
            try:
                floor = list.split('Etage: ')[1].split(' ')[0]
            except:
                pass
        if 'Haustiere' in list:
            try:
                pets_allowed = list.split('Haustiere: ')[1].split(' ')[0]
            except:
                pass
        if 'Nach' in pets_allowed:
            pets_allowed = True
        elif 'Nein' in pets_allowed:
            pets_allowed = False
        terrace = None
        balcony = None
        elevator = None
        parking = None
        if 'Balkon/ Terrasse:'in list:
            terrace = True
            balcony = True
        if 'Terrassenwohnung' in list:
            terrace = True
        if 'Personenaufzug' in list:
            elevator = True
        if 'Garage' in list or 'Stellplatz' in list:
            parking = True
        furnished = True
        amenities = response.css('.expose--text:nth-child(7) p::text').extract()
        amenities = ' '.join(amenities)
        if 'Waschmaschinen' in amenities:
            washing_machine = True
        else:
            washing_machine = None

        floor_plan_images = None
        try:
            floor_plan_images = response.css('.expose__column__text--image::attr(src)').extract()
            floor_plan_images = ['https:' + x for x in floor_plan_images]
        except:
            pass
        if floor_plan_images == []:
            floor_plan_images = None

        images = response.css('.sp-slides a::attr(href)').extract()
        images = ['https:' + x for x in images]

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

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
        item_loader.add_value("landlord_name", 'Jasmin Ischinger') # String
        item_loader.add_value("landlord_phone", '07051 9691828') # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
