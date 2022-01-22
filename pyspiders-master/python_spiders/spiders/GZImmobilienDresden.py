# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates


class GzimmobiliendresdenSpider(scrapy.Spider):
    name = "GZImmobilienDresden"
    start_urls = ['https://gz-i.de/einraum-wohnungen.html', 'https://gz-i.de/zweiraum-wohnungen.html', 'https://gz-i.de/dreiraum-wohnungen.html']
    allowed_domains = ["gz-i.de"]
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
        for listing in response.css("div.angebot div.caption a::attr(href)").getall():
            yield scrapy.Request(listing, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        washing_machine = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'
        
        data = response.css("div.row")[0].css("div.col-xs-12.col-md-5  div.caption p::text").getall()
        for row in data:
            if len(row.split(':')) < 2: continue
            key = row.split(':')[0].strip()
            val = row.split(':')[1].strip()
            key = key.lower()
            if "objekttyp" in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'haus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                elif 'apartment' in property_type.lower():
                    property_type = 'apartment'
                elif 'dach­geschoss' in property_type.lower():
                    property_type = 'apartment'
                else: return
            if "zimmer" in key:
                room_count = int(float(val))
            elif "badezimmer" in key and val.isnumeric():
                bathroom_count = int(float(val))
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "verfügbar" in key:
                if 'vereinbarung' in val.lower():
                    available_date = None
                elif 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif "kaution" in key:
                deposit = get_price(val)
            elif "stellplatz" in key:
                parking = False if 'nein' in val.lower() else True
            elif "miete zzgl" in key:
                rent, currency = extract_rent_currency(val, self.country, GzimmobiliendresdenSpider)
                rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val)
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
        
        if deposit < 10:
            deposit = deposit * rent
        external_id = response.css("div.caption p::text").getall()[0]
        address_meta = response.css("section#angebot div.row")[-1].css("div.caption p[class!='text-justify']::text").getall()
        address = address_meta[0] + ' ' + address_meta[1] if len(address_meta) > 1 else address_meta[0]
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        if address == zipcode:
            address = zipcode + " " + city
        title = response.css("section#angebot h2 > span::text").get().replace('gz-i.de', '').replace(':', '').strip()

        description =  response.css("section#angebot  div.col-xs-12.col-md-7   p ::text").get().strip().replace('www.gz-i.de', ' ')
        amenities = response.css("section#angebot div.row")[-1].css("div.caption p[class='text-justify']::text").get()
        
        lower_description = description.lower() + " " +  amenities.lower()
        if "stellplatz" in lower_description or "garage" in lower_description or "parkhaus" in lower_description or "tiefgarage" in lower_description:
            parking = True
        if 'balkon' in lower_description:
            balcony = True
        if 'aufzug' in lower_description:
            elevator = True
        if 'terrasse' in lower_description:
            terrace = True
        if 'waschmaschinen' in lower_description:
            washing_machine = True
        
        images = response.css("div#carousel div.carousel-inner div.item img::attr(src)").getall()
        floor_plan_images = response.css("section#angebot div[class!='col-md-7'] div.thumbnail div.caption div.text-center img::attr(src)").getall()
        
        landlord_name = [x.strip() for x in response.css("address::text").getall() if x.strip() != ""][0]
        landlord_phone = response.css("a[title='Telefon-Nummer']::text").get().strip()
        landlord_email = response.css("a[title='E-Mailadresse']::text").get().strip()
        
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
