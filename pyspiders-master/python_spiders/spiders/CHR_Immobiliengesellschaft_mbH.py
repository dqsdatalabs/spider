# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import *
from dateutil.parser import parse
from datetime import datetime

class ChrImmobiliengesellschaftMbhSpider(scrapy.Spider):
    name = "CHR_Immobiliengesellschaft_mbH"
    start_urls = ['https://chr-immobilien.de/miete.html']
    country = 'germany'
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
        urls =  response.css('div.image a::attr(href)').getall()
        for url in urls:
            yield scrapy.Request('https://chr-immobilien.de/' + url, callback=self.populate_item)
    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = balcony = terrace = elevator = external_id = furnished = parking = None
        bathroom_count = available_date = deposit = heating_cost = rent = currency = square_meters = utilities = floor = None
        room_count = 1
        property_type = 'apartment'
        keys = response.css('div.dataBox li::text').getall()
        vals = response.css('div.dataBox ul b::text').getall()
        rent_str = response.css('div.dataBox p b::text').get()
        rent, currency = extract_rent_currency(
            rent_str, self.country, ChrImmobiliengesellschaftMbhSpider)
        rent = get_price(rent_str)
        keys_vals = response.css('p.price small::text').getall()
        for row in zip(keys, vals):
            key = row[0].strip().lower()
            val = row[1].strip()
            if 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator='.', scale_separator=',')))
            elif 'zimmer' in key:
                room_count = int(float(val.replace(',', '.')))
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
        for row in keys_vals:
            key, val = row.split(':')
            key = key.lower().strip()
            val = val.strip()
            if 'nebenkosten' in key:
                utilities = get_price(val)
            elif 'kaution' in key:
                deposit = get_price(val)
        if rent == None:
            return
        external_id = response.css('div b:nth-child(1)::text').getall()[-1].split(': ')[1]
        title = response.css('div.label::text').get()
        address = ''.join(response.css('#dataBoxAddress p::text').getall()).strip()
        description = ''.join(response.css('#dataBoxEquipment p::text').getall()).strip()
        description += ''.join(response.css('#dataBoxEnergy p::text').getall()).strip()
        description += response.css('h2.title::text').get()
        description += ''.join(response.css('div.description p::text').getall())
        description += ''.join(response.css('div.cityInfo p::text').getall()
                               ).split('Mehr Informationen')[0]
        images = response.css('div.images img::attr(src)').getall()
        for i in range(len(images)):
            images[i] = 'https://chr-immobilien.de/' + images[i]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude=longitude, latitude=latitude)
        if title is not None:
            if 'vermietet' in title.lower().replace(' ', ''):
                return
        if 'warmmiete' in response.css('#dataBoxPrice p.price span::text').get().lower():
            heating_cost = 0
        item_loader = ListingLoader(response=response)

        if title is None:
            title = 'apartment ' + response.css('h1.type small::text').get()
        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return
        if 'Bad Sachsa' in address:
            landlord_number = '05523 - 95 36 112'
        else:
            landlord_number = '05525 - 17 33'
        landlord_email = 'info@chr-immobilien.de'

        landlord_name = 'CHR Immobiliengesellschaft mbH'
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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format
        get_amenities(description, '', item_loader)
        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
