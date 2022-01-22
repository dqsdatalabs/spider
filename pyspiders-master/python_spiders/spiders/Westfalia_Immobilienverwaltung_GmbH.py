# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates


class WestfaliaImmobilienverwaltungGmbhSpider(scrapy.Spider):
    name = "Westfalia_Immobilienverwaltung_GmbH"
    start_urls = ['https://www.westfalia-gmbh.de/nutzungsart/wohnen/']
    allowed_domains = ["westfalia-gmbh.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_pages)

    # 2. SCRAPING level 2
    def parse_pages(self, response, **kwargs):
        pages = response.css('div.search-navigation a::attr(href)').getall()
        for page in pages:
            yield scrapy.Request('https://www.westfalia-gmbh.de' + page, callback=self.parse_page)

    # 2. SCRAPING level 3
    def parse_page(self, response, **kwargs):
        urls = response.css('a.link-wrap::attr(href)').getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type  = balcony = terrace = elevator = external_id = furnished = parking = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'

        keys = response.css("th::text").getall()
        vals = response.css("td::text").getall()
        for row in zip(keys, vals):
            key = row[0].strip()
            val = row[1].strip()
            key = key.lower()
            if "plz / ort" in key:
                address = val
            elif "straße" in key:
                address += ' ' + val
            elif "netto-kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, WestfaliaImmobilienverwaltungGmbhSpider)
                rent = get_price(val)
            elif "heizkosten" in key:
                heating_cost = get_price(val)
            elif "betriebskosten" in key:
                utilities = get_price(val)
            elif "kaution" in key:
                deposit = get_price(val)
            elif "zimmer" in key:
                room_count = int(float(val.replace(',', '.')))
            elif 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
            elif 'objektnummer' in key:
                external_id = val
        if rent is None:
            return

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = response.css("div.gallery-wrap h1::text").get().strip()
        des = response.css('div.info-wrap.part-2 p::text').getall()
        des_ul = response.css('div.info-wrap.part-2 ul li::text').getall()
        description = ''
        for item in des:
            description = item + ' '
        for item in des_ul:
            description += item + ' '
        images = response.css('div.gallery img::attr(src)').getall()
        floor_plan_images = [images[-1]]
        images = images[:len(images) - 1]

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
        if response.css('div.info-wrap.part-2 h2::text').getall()[1] == 'Ausstattung':
            furnished = True

        landlord_name = 'Westfalia Immobilienverwaltung GmbH'
        landlord_number = '0800 757500100'

        
        item_loader = ListingLoader(response=response)

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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
def get_price(val):
    v = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price
