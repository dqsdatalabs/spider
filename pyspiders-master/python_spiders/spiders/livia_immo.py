# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class LiviaImmoSpider(scrapy.Spider):
    name = "livia_immo"
    start_urls = ['https://www.livia-immo.de/mieten/wohnen']
    allowed_domains = ["livia-immo.de"]
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
        property_urls = response.css('.item-image a::attr(href)').extract()
        property_urls = set(property_urls)
        for property_url in property_urls:
            if '#' in property_url:
                continue
            property_url = 'https://www.livia-immo.de/' + property_url
            yield Request(url=property_url, callback=self.populate_item)
        try:
            next_page = response.css('.imo-pagination .uk-pagination li.uk-active + li a::attr(onclick)')[-1].extract()
            next_page = int(''.join(x for x in next_page if x.isdigit()))
            yield Request(url=f'https://www.livia-immo.de/mieten/wohnen/immobilie-zum-wohnen-mieten/{next_page}', callback=self.parse)
        except:
            pass
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('.uk-text-break ::text')[0].extract()
        title = remove_white_spaces(title)
        description = response.css('#tab-default > div > div:nth-child(4) .uk-text-break ::text, #tab-default > div > div:nth-child(5) .uk-text-break ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        address = response.css('.default-incl-gmap ::attr(data-address)')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        list = response.css('#tab-default > div > div:nth-child(3) ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        if 'Kaltmiete ' in list:
            rent = list.split('Kaltmiete ')[1].split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        utilities = None
        if 'Nebenkosten ' in list:
            utilities = list.split('Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        deposit = None
        if 'KM Kaution ' in list:
            deposit = list.split('KM Kaution ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        square_meters = None
        if 'Wohnfläche ca. ' in list:
            square_meters = list.split('Wohnfläche ca. ')[1].split('m')[0]
            if ',' in square_meters:
                square_meters = square_meters.split(',')[0]
            square_meters = int(square_meters)
        room_count = 1
        if 'Zimmer  ' in list:
            room_count = list.split('Zimmer  ')[1].split(' ')[0]
            room_count = int(room_count)
        bathroom_count = 1
        if 'Badezimmer  ' in list:
            bathroom_count = list.split('Badezimmer  ')[1].split(' ')[0]
            bathroom_count = int(bathroom_count)

        external_id = None
        if 'Objekt-Nr. ' in list:
            external_id = list.split('Objekt-Nr. ')[1].split(' ')[0]
        available_date = None
        if 'verfügbar ab' in list:
            available_date = list.split('verfügbar ab ')[1].split(' ')[0]
            try:
                import datetime
                import dateparser
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

        balcony = None
        if 'balkon' in list.lower():
            balcony = True
        terrace = None
        if 'terrase' in list.lower():
            terrace = True
        elevator = None
        if 'aufzug' in list.lower():
            elevator = True
        washing_machine = None
        if 'waschmasch' in description.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirrspüler' in description.lower():
            dishwasher = True
        parking = None
        if 'stellplatz' in description.lower():
            parking = True

        images = response.css('.uk-slideshow-items img::attr(src)').extract()
        images = ['https://www.livia-immo.de/' + x for x in images]
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
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
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
        item_loader.add_value("landlord_name", 'Livia Immobilien') # String
        item_loader.add_value("landlord_phone", '0341 9625107') # String
        item_loader.add_value("landlord_email", 'info@livia-immo.de') # String

        self.position += 1
        yield item_loader.load_item()
