# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates

class LeipzigWohnen24Spider(scrapy.Spider):
    name = "LEIPZIG_WOHNEN24"
    start_urls = ['https://www.leipzig-wohnen24.de/de/index_0__1_sp1_1.html']
    allowed_domains = ["leipzig-wohnen24.de"]
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
        urls = response.css('div.galerieansicht_bilder a::attr(href)').getall()

        for url in urls:
            yield scrapy.Request('https://www.leipzig-wohnen24.de'+ url, callback=self.populate_item)


    def get_price(self, val):
        v = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
        v2 = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
        price = min(v, v2)
        if price < 10:
            price = max(v, v2)
        return price
    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'
        
        keys = response.css("div.eze2::text").getall()
        vals = response.css("div.iaus3")
        
        for row in zip(keys, vals):
            key = row[0].strip()
            val = row[1].css('::text').get()
            key = key.lower()
            if 'wohnungstyp' in key or 'küche' in key:
                continue
            if 'adresse' in key:
                address = val + ' '
            elif 'stadtteil' in key:
                address += val
            elif 'kaltmiete' in key:
                rent, currency = extract_rent_currency(val, self.country, LeipzigWohnen24Spider)
                rent = self.get_price(val)
            elif 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif 'schlafzimmer' in key:
                room_count = int(float(val.replace(',', '.')))
            elif 'warmmiete' in key:
                heating_cost = self.get_price(val) - rent
            elif 'nebenkosten' in key:
                utilities = self.get_price(val)                    
            elif 'heizkosten' in key:
                heating_cost = 0
            elif 'badezimmer' in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif 'balkon' in key:
                balcony = True
            elif 'parkflächen' in key:
                parking = True
            elif 'haustiere' in key:
                pets_allowed = True
            elif 'bezugsfrei ab' in key:
                if 'vermietet' in val.lower():
                    return
                elif 'rücksprache' in val.lower():
                    available_date = None
                elif 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif 'objektbeschreibung' in key:
                description = val
            elif 'ausstattung' in key:
                furnished = True
                description += ' ' + val
            elif '' == key:
                val = row[1].css('::text').getall()
                landlord_name = val[0] + val [1]
            elif 'lage' in key:
                description += ' ' + val
        if rent == None:
            return
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        if address == zipcode:
            address = zipcode + " " + city
        title = response.css("div.headline_1 h1::text").get().strip()

        lower_description = description.lower()
        if "stellplatz" in lower_description or "garage" in lower_description or "parkhaus" in lower_description or "tiefgarage" in lower_description:
            parking = True
        if 'balkon' in lower_description:
            balcony = True
        if 'aufzug' in lower_description:
            elevator = True
        if 'terrasse' in lower_description:
            terrace = True
        if 'waschmaschine' in lower_description:
            washing_machine = True
        
        images = response.css('li::attr(data-bottom-thumb)').getall()
        images = ['https://www.leipzig-wohnen24.de'+ image for image in images]
        floor_plan_images = None
        for img in images:
            if '-gr-' in img:
                floor_plan_images = ['https://www.leipzig-wohnen24.de'+ img]
                images.remove(img)
        landlord_email = 'vermietung@leipzig-wohnen24.de'
        landlord_phone = '+49 152 02001039'

        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
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
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
