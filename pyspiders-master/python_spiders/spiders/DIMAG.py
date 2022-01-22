# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, format_date, extract_rent_currency, extract_location_from_address, remove_white_spaces, extract_location_from_coordinates


class DimagSpider(scrapy.Spider):
    name = "DimagDresden"
    start_urls = ['https://www.dimag-dresden.de/aktuelle-angebote/mieten/seite/1/']
    allowed_domains = ["dimag-dresden.de"]
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
        for listing in response.css("ul.object__list > li > a::attr(href)").getall():
            yield scrapy.Request('https://www.dimag-dresden.de/' + listing, callback=self.populate_item)
        next_page = response.css("a.arrow.arrow--next::attr(href)").get()
        if next_page:
            yield scrapy.Request('https://www.dimag-dresden.de/' + next_page, callback=self.parse) 

    # 3. SCRAPING level 3
    def populate_item(self, response):
        
        property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = address = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        keys = response.css("li.details__element i::text").getall()
        vals = response.css("li.details__element strong::text").getall()
        for row in zip(keys, vals):
            key = row[0].strip()
            val = row[1].strip()
            key = key.lower()
            if "immobilienart" in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'haus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                else: return
            if "objekt-id" in key:
                external_id = val
            elif key.find("zimmer") != -1:
                room_count = int(float(val))
            elif "badezimmer" in key and val.isnumeric():
                bathroom_count = int(float(val))
            elif "etage" in key:
                floor = val
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "bezugstermin" in key:
                if 'sofort' in val:
                    available_date = datetime.now().strftime("%Y-%m-%d")
                elif len(val) == 4:
                    available_date = parse('01-01-{}'.format(val)).strftime("%Y-%m-%d")
                else:
                    val = val.replace('ab', '')
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif "kaution" in key:
                deposit = int(float(extract_number_only(val)))
            elif "stellplatzart(en)" in key:
                parking = True
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, DimagSpider)
            elif "warmmiete" in key:
                total_rent, _ = extract_rent_currency(val, self.country, DimagSpider)
            elif "aufzug" in key:
                elevator = True
            elif "balkon" in key:
                balcony = True
            elif key.find("terrassen") != -1:
                terrace = True
            elif 'lage' in key:
                address = val
            elif 'haustiere erlaubt' in key:
                pets_allowed = True if val.lower() == 'ja' else False
                
        amenities = response.css("li.benefits__element > p::text").getall()     
        for amenity in amenities:
            if "stellplatz" in amenity.lower() or "garage" in amenity.lower() or "parkhaus" in amenity.lower() or "tiefgarage" in amenity.lower():
                parking = True
            elif "aufzug" in amenity.lower():
                elevator = True
            elif "balkon" in amenity.lower():
                balcony = True
            elif "terrassen" in amenity.lower():
                terrace = True
            elif 'haustiere' in amenity.lower():
                pets_allowed = True
                
        utilities = total_rent - rent
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = re.sub(re.compile('<.*?>'), '', response.css("h1.headline--base").get()).strip()

        description =  '. '.join(response.css("div.accordion--content")[1].css("li.benefits__element p::text").getall())
        
        # alt!=Top-Makler
        images = list(set(response.css("li figure img[alt!='Top-Makler']::attr(src)").getall()) - set(response.css("li figure img[alt='Grundriss']::attr(src)").getall()))
        images = ['https://www.dimag-dresden.de/'+image for image in images]
        floor_plan_images = response.css("figure img[alt='Grundriss']::attr(src)").getall()
        floor_plan_images = ['https://www.dimag-dresden.de/'+flor_plan_image for flor_plan_image in floor_plan_images]
        
        landlord_name = ' '.join(response.css("div.contact--data>*::text").getall())
        contact_info = response.css("div.contact--data span::text").getall()
        for info in contact_info:
            if '@' in info:
                landlord_email = info
            else:
                landlord_phone = info
        
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
