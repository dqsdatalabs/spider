# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup

class ImmoPoppSpider(scrapy.Spider):
    name = "immo_popp"
    start_urls = ['https://www.immobilien-popp.de/immobilienangebote-mietwohnung-oder-efh-haus-zum-mieten-und-kaufen-region-greiz-vogtland?vermarktungsart_f%5B%5D=Mieten+oder+Pachten']
    allowed_domains = ["immobilien-popp.de"]
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
        property_urls = response.css('.item a::attr(href)').extract()
        property_urls = ['https://www.immobilien-popp.de/' + x for x in property_urls]
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list = response.css(".label::text , .value::text").extract()
        list = ' '.join(list)
        if 'kaltmiete' in list.lower():
            rent = list.split("Kaltmiete ")[1].split(",")[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        is_comm = None
        if 'Nutzungsart' in list:
            is_comm = list.split("Nutzungsart ")[1].split(" ")[0]
        if 'Wohnen' not in is_comm:
            return
        zipcode = None
        if 'PLZ' in list:
            zipcode = list.split("PLZ ")[1].split(" ")[0]
        city = None
        if 'Ort' in list:
            city = list.split("Ort ")[1].split(" ")[0]
        description = response.css("#article-71 p ::text")[:-1].extract()
        description = '  '.join(description)
        if 'Anschrift' in description:
            address = description.split("Anschrift: ")[1].split('  ')[0]
        else:
            address = zipcode + ' ' + city
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        title = response.css("h1 ::text")[0].extract()
        balcony = None
        if 'balkon' in title.lower():
            balcony = True
        property_type = 'apartment'
        square_meters = None
        if 'Wohnfläche' in list:
            square_meters = list.split("Wohnfläche ")[1].split(",")[0]
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        elif 'Gesamtfläche' in list:
            square_meters = list.split("Gesamtfläche ")[1].split(",")[0]
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        else:
            pass
        room_count = None
        if 'Zimmer ' in list:
            room_count = list.split("Zimmer ")[1].split(" ")[0]
            room_count = int(''.join(x for x in room_count if x.isdigit()))
            if room_count == 1:
                property_type = 'room'
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split("Nebenkosten ")[1].split(",")[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        deposit = None
        if 'Kauktion' in list:
            deposit = list.split("Kauktion ")[1].split(",")[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))

        external_id = None
        if 'Objekt-Nr.' in list:
            external_id = list.split("Objekt-Nr. ")[1].split(" ")[0]

        bathroom_count = None
        if 'Bad' in list:
            bathroom_count = 1
        swimming_pool = None
        if 'bad' in description.lower():
            swimming_pool = True
        washing_machine = None
        if 'Waschmaschine ' in description:
            washing_machine = True
        furnished = None
        if 'sanierte ' in description:
            furnished = True
        parking = None
        if 'Parkmöglichkeiten' in description or 'Stellplätze ' in description:
            parking = True


        energy_label = None
        if 'Energieeffizienzklasse' in description:
            energy_label = description.split("Energieeffizienzklasse: ")[1].split(' \n')[0]
        elif 'Energiekennwert' in description:
            energy_label = int(description.split("Energiekennwert: ")[1].split(' kWh/(m²*a)')[0])
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

        images = response.css('.cboxElement img::attr(src)').extract()
        images = ['https://www.immobilien-popp.de/' + x for x in images]

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

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Maklerbüro Jan Popp & Immobilienverwaltung Jan Popp e.K.') # String
        item_loader.add_value("landlord_phone", '03661 670609') # String
        item_loader.add_value("landlord_email", 'info@immobilien-popp.de') # String

        self.position += 1
        yield item_loader.load_item()
