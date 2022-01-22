# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup

class KuenneImmoSpider(scrapy.Spider):
    name = "kuenne_immo"
    start_urls = ['https://www.kuenne-immobilien.de/vermietung/wohnung/?filter%5B1%5D%5Bmeta_query%5D%5B0%5D']
    allowed_domains = ["kuenne-immobilien.de"]
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
        property_urls = response.css('.article-header a::attr(href)').extract()
        titles = response.css('.article-header a::text').extract()
        for index, property_url in enumerate(property_urls):
            yield Request(url=property_url, callback=self.populate_item, meta={'title': titles[index]})
        last_page = response.css('.page-last::attr(href)')[0].extract()
        current_page = response.url
        page_next = response.css('.page-next::attr(href)')[0].extract()
        if current_page != last_page:
            yield Request(url=page_next, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta["title"]
        address = response.css('.object-location ::text').extract()
        address = ''.join(address).strip()
        address = remove_white_spaces(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        balcony = None
        if 'balkon' in title.lower():
            balcony = True
        external_id = response.css('.spacer-bottom-24:nth-child(1) div+ div ::text')[0].extract()
        mini_list = response.css('.object-info-value::text').extract()
        rent = str(mini_list[-1])
        if ',' in rent:
            rent = rent.split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        room_count = mini_list[0].strip()
        if ',' in room_count:
            room_count = int(room_count[0]) + 1
        else:
            room_count = int(room_count)
        square_meters = mini_list[1]
        square_meters = square_meters[:-2].strip()
        if ',' in square_meters:
            square_meters = square_meters.split(',')[0]
        square_meters = int(square_meters)

        description = response.css('.margin-md-top p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        dishwasher = None
        if 'geschirrspülmaschine' in description.lower():
            dishwasher = True
        if 'balkon' in description.lower():
            balcony = True
        bathroom_count = 1
        list = response.css('td ::text').extract()
        list = ''.join(list)
        list = remove_white_spaces(list)
        terrace = None
        if 'terrasse' in list.lower():
            terrace = True
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        deposit = None
        if 'Kaution' in list:
            deposit = list.split('Kaution ')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        energy_label = None
        if 'Energieeffizienzklasse' in list:
            energy_label = list.split('Energieeffizienzklasse ')[1].split(' ')[0]
        floor = None
        if ' Geschoss' in list:
            floor = list.split(' Geschoss')[1].split('.')[0]

        contact = response.css('.spacer-bottom-24+ .spacer-bottom-24 div ::text').extract()
        try:
            landlord_name = contact[1]
            landlord_phone = contact[2]
            landlord_email = contact[4]
            landlord_phone = landlord_phone.split('Tel:')[1].strip()
        except:
            landlord_name = 'Künne Real Estate Group'
            landlord_phone = '+49 (0) 341 442 95 66'
            landlord_email = 'info@kuenne-immobilien.de'
        images = response.css('.ms-slide img::attr(data-src)').extract()
        floor_plan_images = response.css('.size-full ::attr(src)').extract()
        if len(floor_plan_images) == 0:
            floor_plan_images = None

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
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
