# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class CarlcfranzenSpider(scrapy.Spider):
    name = "CarlCFranzen"
    start_urls = ['https://www.ivd24immobilien.de/objektlisten/index.php?oid=381&page=1']
    allowed_domains = ["ivd24immobilien.de"]
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
        next_page = response.css('.ivd24-pagination .ivd24-link::attr(data-href)').getall()[-1]
        for rental in response.css('.ivd24-result'):
            external_link_id = rental.css('a::attr(href)').extract_first()
            yield scrapy.Request(url='https://www.ivd24immobilien.de/objektlisten/show.php?oid=381&id={}'.format(external_link_id.split('-')[-1]), callback=self.populate_item,  meta={'external_id' : external_link_id, 'external_link' : 'https://ccfranzen.de/immobilienangebot' +external_link_id})
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        heating_cost = energy_label =  utilities = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = landlord_email = landlord_name = landlord_phone = None
        room_count = bathroom_count = 1
        title = response.css('#ivd24-expose-title::text').extract_first()
        for box in response.css('.ivd24-content-box'):
            if box.css('.ivd24-bold::text').extract_first() and box.css('.ivd24-bold::text').extract_first() == 'Objektbeschreibung':
                description = '\n'.join([x for x in box.css(' ::text').getall() if '@' not in x and ' unter ' not in x and ' Tel' not in x]).replace('Objektbeschreibung','').strip()
        
        lower_description = '\n'.join(response.css("div.textimg").extract()).lower() + ' ' +  title.lower()
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
            key = row[0].css("::text").get()
            val = row[1].css("::text").get()
            if val is not None: val = val.strip()
            key = key.lower()
            if 'objektnummer' in key:
                external_id = val
            elif 'objektanschrift' in key:
                address = remove_white_spaces(val.strip())
            elif 'objektart' in key and 'miete' not in val.lower():
                return                
            elif 'objekttyp' in key:
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
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, CarlcfranzenSpider)
                rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val) if get_price(val) !=0 else utilities
            elif "heizkosten" in key:
                heating_cost = get_price(val)
            elif "kaution" in key:
                deposit = get_price(val)
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "anzahl zimmer" in key:
                room_count = int(float(val.replace(',', '.')))
            elif "anzahl badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif "verfügbar ab" in key:
                if 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                elif 'vermietet' in val.lower():
                    return
                else:
                    try:
                        available_date = parse(val.split(' ')[0]).strftime("%Y-%m-%d")
                    except:
                        available_date = None  
            elif 'wertklasse' in key:
                energy_label = val           
            elif "balkon" in key:
                balcony = True
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(val, self.country, CarlcfranzenSpider)
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


        longitude, latitude = extract_location_from_address(address)
        zipcode, city, a = extract_location_from_coordinates(longitude=longitude, latitude=latitude)

        images = []
        images.extend([x.replace('thumb', 'web') for x in response.css("div#ivd24-expose-image img::attr(src)").getall()])
        images.extend([x.replace('thumb', 'web') for x in response.css("div#ivd24-expose-thumbnails img::attr(src)").getall()])
            
        floor_plan_images = None

        landlord_phone = '(040) 320 22 66'
        landlord_email = 'info@ccfranzen.de'
        landlord_name = 'Carl C. Franzen (GmbH & Co. KG)'

        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.meta['external_link']) # String
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

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
