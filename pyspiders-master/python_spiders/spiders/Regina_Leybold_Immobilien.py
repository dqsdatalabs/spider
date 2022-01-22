# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import description_cleaner, extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, get_amenities, get_price

class ReginaLeyboldImmobilienSpider(scrapy.Spider):
    name = "Regina_Leybold_Immobilien"
    start_urls = ['https://www.immobilien-leybold.de/immobilien/']
    allowed_domains = ["immobilien-leybold.de"]
    country = 'germany' # Fill in the Country's name
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
        urls = response.css('article a::attr(href)').getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        heating_cost = washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = energy_label = None
        utilities  = available_date = deposit = total_rent = rent = currency = square_meters = total_square_meters = landlord_email = landlord_name = landlord_phone = None
        room_count = 1
        bathroom_count = 1
        property_type = 'apartment'

        table = response.css('div.FFestateview-default-details-content-details li')
        keys = []
        vals = []
        for t in table:
            keys.append(t.css('span:nth-child(1)::text').get().strip().lower())
            vals.append(t.css('span:nth-child(2)'))
        amenties = ''
        for row in zip(keys, vals):
            key = row[0]
            val = row[1].css('::text').get().strip().lower()
            if 'art' in key:
                if 'wohnung' in val:
                   property_type = 'apartment'
                else:
                    property_type = 'house'
            elif 'zugang ab' in key:
                available_date = parse(val).strftime("%Y-%m-%d")
            elif 'wohnfläche' in key:
                val = ''.join(row[1].css('::text').getall()).strip()
                square_meters = int(float(extract_number_only(
                    val, thousand_separator=',', scale_separator='.')))
            elif 'grundstück' in key:
                val = ''.join(row[1].css('::text').getall()).strip()
                total_square_meters = int(float(extract_number_only(
                    val, thousand_separator=',', scale_separator='.')))
            elif 'immobilien-id' in key:
                external_id = val
            elif "badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif "zimmer" in key:
                room_count = int(float(val.split(',')[0]))
            elif 'lage' in key:
                address = val
            elif 'etagenzahl' in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif 'balkone' in key:
                if int(val[0]) > 0:
                    amenties += 'balkone'
            elif 'stellplatzanzahl' in key:
                if int(val[0]) > 0:
                    amenties += ' Stellplatzanzahl'
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(
                    val, self.country, ReginaLeyboldImmobilienSpider)
                rent = get_price(val)
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(
                    val, self.country, ReginaLeyboldImmobilienSpider)
                total_rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val)
            elif 'heizkosten' in key:
                heating_cost = get_price(val)
        description_amenties = response.css('div.FFestateview-default-details-content-description p::text').getall()
        description = description_cleaner(description_amenties[0])
        title = response.css('h1.entry-title.main_title::text').get()
        if 'RESERVIERT' in title.upper():
            return
        amenties += ' '.join(description_amenties[1:])
        if rent is None:
            if total_rent is not None: 
                rent = total_rent
            else:
                return
        if square_meters is None:
            if total_square_meters is not None: 
                square_meters = total_square_meters

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude=longitude, latitude=latitude)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        images = response.css('div.FFestateview-default-details-nav img::attr(data-lazy)').getall()
        landlord_number = response.css('div.FFestateview-default-details-agent-contact a::text').get().strip()
        landlord_name = response.css('div.FFestateview-default-details-agent-name span::text').get().strip()
    
        item_loader = ListingLoader(response=response)
    
        # Enforces rent between 0 and 40,000 please dont delete these lines
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        get_amenities(description, amenties, item_loader)
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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", 'EUR') # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
