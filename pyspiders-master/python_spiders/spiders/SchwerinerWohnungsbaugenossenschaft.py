# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, remove_white_spaces


class SchwerinerwohnungsbaugenossenschaftSpider(scrapy.Spider):
    name = "SchwerinerWohnungsbaugenossenschaft"
    start_urls = ['https://www.swg-schwerin.de/wohnen/wohnungssuche']
    allowed_domains = ["swg-schwerin.de"]
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
        for listing in  response.css("article.real-estate"):
            yield scrapy.Request('https://www.swg-schwerin.de'+listing.css("h3 a::attr(href)").get(), callback=self.populate_item)
        next_page = response.css("a[rel='next']::attr(href)").get()
        if next_page:
            yield scrapy.Request('https://www.swg-schwerin.de/wohnen/wohnungssuche'+next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        floor_plan_images = washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        heating_cost = energy_label =  utilities = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = landlord_email = landlord_name = landlord_phone = None
        district = zip_code = street = ''
        room_count = bathroom_count = 1
        property_type = 'apartment'
        address = ''
        title = response.css("h1::text").get().strip()
        description = '\n'.join([x for x in response.css("div.location-text p::text").getall()])

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

        keys = response.css("span.detail-label")
        vals = response.css("span.detail-content")
        for row in zip(keys,vals):
            key = row[0].css("::text").get().strip()
            val = row[1].css("::text").get().strip()
            key = key.lower()
            if 'stadteil' in key:
                district = val
            elif 'plz/ort' in key:
                zip_code = val
            elif 'straße' in key:
                street = val
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "kalt-miete" in key:
                rent, currency = extract_rent_currency(val, self.country, SchwerinerwohnungsbaugenossenschaftSpider)
                rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val) if get_price(val) !=0 else utilities
            elif "anzahl zimmer" in key:
                room_count = int(float(val.replace(',', '.')))
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            if 'objektnummer' in key:
                external_id = val              
            elif "frei ab" in key:
                if 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                elif 'vermietet' in val.lower():
                    return
                else:
                    try:
                        available_date = parse(val.split(' ')[0]).strftime("%Y-%m-%d")
                    except:
                        available_date = None  
            elif 'Effizienzklasse'.lower() in key:
                energy_label = val
            elif "heizung" in key:
                heating_cost = get_price(val) if get_price(val) !=0 else heating_cost
            elif "kaution" in key:
                deposit = get_price(val)
                if deposit < 10:
                    deposit *= rent
            elif 'objektart' in key:
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
            elif "anzahl badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))       
            elif "balkon" in key:
                balcony = True
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(val, self.country, SchwerinerwohnungsbaugenossenschaftSpider)
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

        address = remove_white_spaces(street + ', ' + zip_code + ', ' + district)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, a = extract_location_from_coordinates(longitude=longitude, latitude=latitude)

        images = ['https://www.swg-schwerin.de'+x for x in response.css("div.galerie-item a::attr(href)").getall()]

        landlord_phone = '0385 7450-224'
        landlord_email = 'antje.neuhaeuser@swg-schwerin.de'
        landlord_name = remove_white_spaces(response.css("div.contact-person strong::text").get().strip())
        for index, contact_data in enumerate(response.css("div.contact-person").get().split('\n')):
            if 'phone' in contact_data.lower():
                landlord_phone = response.css("div.contact-person").get().split('\n')[index+1].strip()
            if '@' in contact_data.lower():
                s = contact_data.strip()
                landlord_email = s[s.find('mailto:')+len('mailto:'):s.rfind('">')]
 
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