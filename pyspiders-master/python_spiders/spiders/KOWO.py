# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, remove_unicode_char


class KowoSpider(scrapy.Spider):
    name = "KOWO"
    start_urls = ['https://www.kowo-immobilien.de/mieten.html?immo_page=1"']
    allowed_domains = ["kowo-immobilien.de"]
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
        for listing in response.css("div.row.immolistitem a.noborder::attr(href)").getall():
            yield scrapy.Request('https://www.kowo-immobilien.de' + listing, callback=self.populate_item)
        next_page = response.css("a[title='nächste Seite']::attr(href)").get()
        if next_page:
            yield scrapy.Request('https://www.kowo-immobilien.de' + next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        
        property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = address = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        
        keys = response.css("dt::text").getall()
        vals = response.css("dd::text").getall()
        for key, val in zip(keys, vals):
            key = key.lower().strip()
            val = val.strip()
            if "wohnungstyp" in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'haus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                elif 'dach­geschoss' in property_type.lower():
                    property_type = 'apartment'
                elif 'maisonette' in property_type.lower():
                    property_type = 'apartment'
                else: return
            if "zimmer" in key:
                room_count = int(float(val))
            elif "badezimmer" in key and val.isnumeric():
                bathroom_count = int(float(val))
            elif "etage" in key:
                if 'eg' in val.lower().strip():
                    floor = '0'
                else:
                    floor = val.strip()[0]
                    floor = None if not floor.isnumeric() else floor
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "verfügbar ab" in key:
                if 'vereinbarung' in val.lower():
                    available_date = None
                elif 'sofort' in val:
                    available_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif "kaution" in key:
                deposit = int(float(extract_number_only(val)))
            elif "stellplatz" in key:
                parking = False if 'nein' in val.lower() else True
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, KowoSpider)
            elif "warmmiete" in key:
                total_rent, _ = extract_rent_currency(val, self.country, KowoSpider)
            elif "aufzug" in key:
                elevator = True
            elif "balkon" in key:
                balcony = True
            elif key.find("terrasse") != -1:
                terrace = True
        if rent is None:
            return
        if property_type is None:
            return
        
        address = ' '.join([x.strip() for x in response.css("p.immoaddress::text").getall() if 'objekt' not in x.lower()])
        external_id = response.css("input[name='immocode']::attr(value)").get()    
        utilities = total_rent - rent
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = response.css("h1::text").get().strip()

        description =  response.css("meta[property='og:description']::attr(content)").get()
        
        images = response.css("div.immo-galleria img[title!='Grundriss']::attr(src)").getall()
        images = ['https://www.kowo-immobilien.de'+image for image in images]
        floor_plan_images = response.css("div.immo-galleria img[title='Grundriss']::attr(src)").getall()
        floor_plan_images = ['https://www.kowo-immobilien.de'+flor_plan_image for flor_plan_image in floor_plan_images]
        
        landlord_name = response.css("span.immocontact-name::text").get()
        landlord_email = response.css("span.immocontact-details-email > a::text").get().strip()
        landlord_phone = response.css("span.immocontact-details-phone::text").get().strip()
        
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
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()