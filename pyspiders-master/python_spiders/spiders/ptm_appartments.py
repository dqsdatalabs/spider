# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from datetime import datetime
from datetime import date



class PtmApartmentsSpider(scrapy.Spider):
    name = "ptm_apartments"
    start_urls = ['https://ptm-apartments.de/hannover/','https://ptm-apartments.de/berlin/','https://ptm-apartments.de/stuttgart/','https://ptm-apartments.de/hamburg/']
    allowed_domains = ["ptm-apartments.de"]
    country = 'Germany'
    locale = 'de'
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
        try:
            property_urls = response.css('.property-list:nth-child(1) .info a::attr(href)').extract()
            for property_url in property_urls:
                if 'property' in property_url:
                    yield Request(url=property_url, callback=self.populate_item)
        except:
            return

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        is_rent = response.css('#quick-summary .price::text')[0].extract()
        if 'Price on request' in is_rent:
            return
        list = response.css('dt::text, #quick-summary dd::text').extract()
        list = ''.join(list)
        property_type = list.split("Immobilienart:")[1].split("Status:")[0]
        if 'Studio' in property_type:
            property_type = 'studio'
        else:
            property_type = 'apartment'
        rent = list.split("(€):")[1].split("Bäder:")[0]
        if ',' in rent:
            rent = rent[:-2]
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        square_meters = list.split("Größe:")[1].split(" mZimmer")[0]
        square_meters = int(square_meters.split('-')[0])
        room_count = list.split("mZimmer:")[1].split("(€)")[0]
        if '-' in room_count:
            room_count = int(room_count[4])
        else:
            room_count = int(room_count[0])
        bathroom_count = list.split("Bäder:")[1].split("Telefon:Mobil:")[0]
        bathroom_count = int(bathroom_count[0])
        external_id = response.css('#quick-summary > dl > dd:nth-child(2)::text')[0].extract()
        title = response.css('h1::text')[0].extract()
        description = response.css('p::text').extract()
        description = description[1:]
        description = ' '.join(description)

        amenities = response.css('#property_features li::text').extract()
        amenities = ''.join(amenities)
        parking = None
        if 'Parkplatz' in amenities:
            parking = True
        washing_machine = None
        if 'Waschmaschine' in amenities:
            washing_machine = True
        dishwasher = None
        if 'Geschirrspülmaschine' in amenities:
            dishwasher = True

        address = response.css('.property-title figure::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)
        # floor_plan_images = None
        try:
            floor_plan_images = response.css('#floor-plans > div > a > img::attr(src)').extract()
            if floor_plan_images[0] == '':
                floor_plan_images = None
            for image in floor_plan_images:
                if "-212x155" in image:
                    floor_plan_images = [x.replace('-212x155', '') for x in floor_plan_images]
        except:
            pass
        images = response.css('.property-slide a::attr(href)').extract()

        landlord_name = response.css('.agent-contact-info h3::text')[0].extract()
        landlord_number = response.css('.agent-contact-info dl dd::text')[0].extract()
        landlord_email = response.css('.contact a::attr(href)')[0].extract()
        landlord_email = landlord_email[7:]



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
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
