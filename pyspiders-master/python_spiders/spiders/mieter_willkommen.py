# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class MieterWillkommenSpider(scrapy.Spider):
    name = "mieter_willkommen"
    start_urls = ['https://www.mieter-willkommen.de/ubersicht/']
    allowed_domains = ["mieter-willkommen.de"]
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
        objects = response.css('.wohnung')
        for object in objects:
            property_url = object.css('::attr(href)')[0].extract()
            rent = int(object.css('::attr(data-price)')[0].extract())
            title = object.css('.text .h2 ::text')[0].extract()
            room_count = int(object.css('::attr(data-room)')[0].extract())
            square_meters = int(object.css('::attr(data-area)')[0].extract())
            if room_count == 0 or square_meters == 0 or rent == 0:
                continue
            yield Request(url=property_url, callback=self.populate_item, meta={'rent': rent, 'title': title, 'room_count': room_count, 'square_meters': square_meters})


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta['rent']
        title = response.meta['title']
        room_count = response.meta['room_count']
        square_meters = response.meta['square_meters']
        description = response.css('#beschreibung p ::text')[1:].extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        mini_list = response.css('.addresse ::text').extract()
        mini_list = ' '.join(mini_list)
        mini_list = remove_white_spaces(mini_list)
        address = mini_list.split(' Kaltmiete:')[0]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        latitude = str(latitude)
        longitude = str(longitude)
        energy_label = None
        if 'kwh' in description.lower():
            try:
                energy_label = description.split(' kwh')[0].split('wert:')[1].strip()
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
            except:
                pass

        list = response.css('.panel-default').extract()
        list = ''.join(list)

        balcony = None
        if 'balkon' in title.lower() or 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        parking = None
        if 'stellplätz' in title.lower() or 'stellplätz' in description.lower() or 'stellplätz' in list.lower():
            parking = True
        if 'stellplatz' in title.lower() or 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        washing_machine = None
        if 'wasch' in description.lower() or 'wasch' in list.lower():
            washing_machine = True
        terrace = None
        if 'terrasse' in description.lower() or 'terrasse' in list.lower():
            terrace = True
        elevator = None
        if 'fahrstuhl' in description.lower() or 'fahrstuhl' in list.lower():
            elevator = True
        bathroom_count = None
        if 'bad' in description.lower() or 'bad' in list.lower():
            bathroom_count = 1


        utilities = None
        if 'Nebenkosten' in mini_list:
            utilities = int(mini_list.split('Nebenkosten: ')[1].split(' EUR')[0])
        contact = response.css('.line ::text').extract()
        landlord_name = contact[0]
        if len(contact) >= 4:
            landlord_phone = contact[1]
            landlord_phone = landlord_phone.split('Telefon: ')[1]
            landlord_email = contact[3]
        else:
            landlord_phone = '0351 65656-00'
            landlord_email = None

        images = response.css('.single-slider .item a::attr(href)').extract()


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
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
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
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
