# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates


class ImmoconceptsdeutschlangmbhSpider(scrapy.Spider):
    name = "ImmoconceptsDeutschlanGmbH"
    start_urls = ['https://www.immoconcepts.de/immobilien-angebote']
    allowed_domains = ["immoconcepts.de"]
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
        for listing in response.css("div.card"):
            property_type = listing.css("b::text").get().strip().lower()
            if 'wohnung' in property_type:
                property_type = 'apartment'
            elif 'haus' in property_type:
                property_type = 'house'
            else: 
                continue
            if 'miete' in listing.css("div.fg").get().strip().lower():
                yield scrapy.Request('https://www.immoconcepts.de' + listing.css("a::attr(href)").get() , callback=self.populate_item, meta={'property_type': property_type})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        street = postal = location = utilities = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = landlord_email = landlord_name = landlord_phone = None
        rented = False
        room_count = 1
        property_type = response.meta['property_type']
        title = response.css("div#immodetail h4::text").get().strip()
        try:
            description = '\n'.join([x for x in response.css("div#txt_166 div.panel-body > p").getall() if 'mobil' not in x.lower() and 'immoconcepts' not in x.lower() and 'rufen' not in x.lower()])
        except:
            description = ''
        external_id = response.url.split('-')[-1].split('.')[0]
        
        lower_description = description.lower() + ' ' +  title.lower()
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
            
        keys = response.css("dt")
        vals = response.css("dd")
        for row in zip(keys, vals):
            key = row[0].css("::text").get().strip()
            val = row[1].css("::text").get().strip()
            key = key.lower()
            if "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, ImmoconceptsdeutschlangmbhSpider)
                rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val) if get_price(val) !=0 else utilities
            elif "kaution" in key:
                deposit = get_price(val)
            elif 'straße' in key:
                street = val
            elif 'ort' in key:
                location = val
            elif 'postleitzahl' in key:
                postal = val
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif "zimmer" == key:
                room_count = int(float(val.replace(',', '.')))
            elif 'objekt-nr' in key:
                external_id = val
            elif "verfügbar ab" in key:
                val = val.split(' ')
                for possible_val in val:
                    if 'sofort' in possible_val.lower():
                        available_date = datetime.now().strftime("%Y-%m-%d")
                    elif 'vermietet' in possible_val.lower():
                        return
                    else:
                        try:
                            available_date = parse(possible_val).strftime("%Y-%m-%d")
                        except:
                            pass
            elif 'vermietet' in key:
                if 'ja' in val.lower():
                    rented = True               
            elif "objekttyp" in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'familienhaus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                elif 'apartment' in property_type.lower():
                    property_type = 'apartment'
                elif 'dach­geschoss' in property_type.lower():
                    property_type = 'apartment'
                elif 'zimmer' in property_type.lower():
                    property_type = 'room'
                else: return
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "balkon" in key:
                balcony = True
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(val, self.country, ImmoconceptsdeutschlangmbhSpider)
                total_rent = get_price(val)
                
            elif "stellplatz" in key:
                parking = False if 'nein' in val.lower() else True
            elif "aufzug" in key:
                elevator = True
            elif key.find("terrasse") != -1:
                terrace = True if 'terrasse' in val.lower() else None
            elif 'wasch' in key:
                washing_machine = True
            elif 'haustiere' in key:
                pets_allowed = False if 'nein' in val.lower() else True

        if available_date is None and rented == True:
            return
        if rent is None:
            return
        address =  postal + ' ' + location + ' ' + street
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        if address == zipcode:
            address = zipcode + " " + city

        
        images = response.css("div.row.text-center.text-lg-left img[alt!='Grundriss']::attr(src)").getall()
        floor_plan_images = response.css("div.row.text-center.text-lg-left img[alt='Grundriss']::attr(src)").getall()
        if len(floor_plan_images) == 0:
            floor_plan_images = None
        landlord_name = 'IMMOCONCEPTS Deutschland GmbH & Co. KG'
        landlord_email = 'info@immoconcepts.de'
        landlord_phone = '+49 (0)2933 - 909 30 50'
        
        
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
        item_loader.add_value("washing_machine", washing_machine) # Boolean
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


def get_price(val):
    v = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price
