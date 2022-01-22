# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup
import json
import datetime
import dateparser

class GewobaDeSpider(scrapy.Spider):
    name = "gewoba_de"
    start_urls = ['https://www.gewoba.de/mieten-verwalten-kaufen-verkaufen/wohnung-mieten']
    allowed_domains = ["gewoba.de"]
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(
            url='https://www.gewoba.de/typo3conf/ext/iu_properties/Resources/Public/Js/propertiesJson.js',
            callback=self.parse,
            body='',
            method='GET')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        body = str(response.body)
        body = body.split('var jsonObj = ')[1]
        body = body[:-1]
        parsed_response = json.loads(body)
        for item in parsed_response['properties']:
            url = item['_uri']
            url = 'https://www.gewoba.de/' + url.replace('\\', '').strip()
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta["item"]
        title = item['titel']
        latitude = item['lat']
        longitude = item['lng']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if city == '':
            address = response.css('.prop-location .data ::text')[0].extract()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            longitude = str(longitude)
            latitude = str(latitude)
            print(longitude)
        if title == '':
            title = address
        available_date = item['verfuegbar_seit']
        try:
            available_date = available_date.strip()
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        except:
            available_date = None
        room_count = item['raeume']
        floor = item['lage_im_haus']['value']
        square_meters = item['nutz_flaeche_q_m']['value']
        if ',' in square_meters:
            square_meters = square_meters.split(',')[0]
        square_meters = int(square_meters)
        rent = response.css('.prop-topinfo-item:nth-child(1) .data-numeric ::text')[0].extract()
        if ',' in rent:
            rent = rent.split(',')[0]
        rent = int(''.join(x for x in rent if x.isdigit()))
        list = response.css('.prop-detail-data span ::text').extract()
        list = ' '.join(list)
        external_id = None
        list = remove_white_spaces(list)
        if 'Objekt-Nr.' in list:
            external_id = list.split('Objekt-Nr.: ')[1].split(' ')[0]
        utilities = None
        if 'Nebenkosten­vorauszahlung' in list:
            utilities = int(list.split('Nebenkosten­vorauszahlung ')[1].split(',')[0])
        heating_cost = None
        if 'Heizkosten­vorauszahlung' in list:
            heating_cost = int(list.split('Heizkosten­vorauszahlung ')[1].split(',')[0])
        mini_list = response.css('.prop-label ::text, #page-main li ::text').extract()
        mini_list = ' '.join(mini_list)
        mini_list = remove_white_spaces(mini_list)
        description = response.css('.data-text::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)

        balcony = None
        if 'balkon' in mini_list.lower() or 'balkon' in title.lower() or 'balkon' in description.lower() :
            balcony = True
        elevator = None
        if 'aufzug' in mini_list.lower() or 'aufzug' in title.lower() or 'aufzug' in description.lower() :
            elevator = True

        floor_plan_images = response.css('.pswp-groundplans img::attr(src)').extract()
        images = response.css('.pswp-images img::attr(src)').extract()
        if len(images) == 0:
            images = ['https://www.gewoba.de/fileadmin/_processed_/9/f/csm_IMG0007_1119b59fa6.webp']
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
        item_loader.add_value("bathroom_count", 1) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'GEWOBA Service') # String
        item_loader.add_value("landlord_phone", '0421 36 72 - 590') # String
        item_loader.add_value("landlord_email", 'info@gewoba.de') # String

        self.position += 1
        yield item_loader.load_item()
