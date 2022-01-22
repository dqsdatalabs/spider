# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from ..helper import extract_number_only, format_date, extract_rent_currency, extract_location_from_address, remove_white_spaces, extract_location_from_coordinates

class EquipegmbhSpider(scrapy.Spider):
    name = "EquipeGmbH"
    start_urls = ['https://equipe-deutschland.de/suche/index.php?page=1']
    allowed_domains = ["equipe-deutschland.de"]
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
        for listing in response.css("a.eobjekt"):
            property_type = listing.css("div.name::text").get()
            if property_type.lower() not in ['wohnung', 'haus']: continue
            yield scrapy.Request('https://equipe-deutschland.de/' + listing.attrib['href'], callback=self.populate_item, meta={'property_type': property_type})
        next_page = response.css("ul#pagination > li")[-1].css("a::attr(href)").get()
        if next_page:
            yield scrapy.Request('https://equipe-deutschland.de' + next_page, callback=self.parse) 

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = [re.sub(re.compile('<.*?>'), '', x) for x in response.css("div.infobox > p").getall()]
        property_type = response.meta["property_type"]
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
        elif 'haus' in property_type.lower():
            property_type = 'house'
        
        pets_allowed = balcony = terrace = elevator = external_id = floor = parking = address = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        
        for row in data:
            if len(row.split(':')) < 2: continue
            key = row.split(':')[0].strip()
            val = row.split(':')[1].strip()
            key = key.lower()
            if "kauf" in key:
                return
            if "objektnummer" in key:
                external_id = val
            elif key.find("zimmeranzahl") != -1:
                room_count = int(float(val))
            elif "badezimmer" in key and val.isnumeric():
                bathroom_count = int(float(val))
            elif "etage" in key:
                floor = val
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
            elif "verfügbar ab" in key:
                available_date = parse(val).strftime("%Y-%m-%d")
            elif "kaution" in key:
                deposit = int(float(extract_number_only(val)))
            elif "stellplatzart(en)" in key:
                parking = True
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, EquipegmbhSpider)
            elif "warmmiete" in key:
                total_rent, _ = extract_rent_currency(val, self.country, EquipegmbhSpider)
            elif "aufzug" in key:
                elevator = True
            elif "balkon" in key:
                balcony = True
            elif key.find("terrassen") != -1:
                terrace = True
            elif 'standort' in key:
                address = val
            elif 'haustiere erlaubt' in key:
                pets_allowed = True if val.lower() == 'ja' else False
                
        utilities = total_rent - rent
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = response.css("h1::text").get()

        description = ' '.join([x.strip() for x in response.css("article#hauptbeschreibung > p::text").getall() if 'equipe-deutschland' not in x])
        
        
        images = response.css("ul#bildergalerie > li  img[alt!='Premiumpartner 2021']::attr(src)").getall()
        images = ['https://equipe-deutschland.de/'+image for image in images]
        landlord_phone = response.css("span[property='telephone']::text").get()
        landlord_email = response.css("span[property='email']::text").get()
        
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
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
        item_loader.add_value("landlord_name", "Équipe GmbH") # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
