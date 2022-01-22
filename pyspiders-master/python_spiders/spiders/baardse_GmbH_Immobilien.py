# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import description_cleaner, extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, get_amenities
from dateutil.parser import parse
from datetime import datetime
from scrapy.http import HtmlResponse


class BaardseGmbhImmobilienSpider(scrapy.Spider):
    name = "baardse_GmbH_Immobilien"
    start_urls = [
        'https://www.baardse-immobilien.de/kaufen/immobilienangebote']
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

        urls = response.css('div.immo-listing__wrapper a.immo-listing__image::attr(href)').getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)
        next_page = response.css(
            'div.pages a.next.page-numbers::attr(href)').get()
        if next_page != None:
            yield scrapy.Request(next_page, self.parse)
        

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = response.css('span.badge.badge-secondary::text').get()
        if 'Kauf' in property_type:
            return
        if 'Wohnung' in property_type:
            property_type = 'apartment'
        elif 'Haus' in property_type:
            property_type = 'house'
        else:
            return
        address = response.css('p.h5::text').get().strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude=longitude, latitude=latitude)
        title = response.css('div.row h1::text').get().strip()
        external_id = response.css('span.lh-large::text').get().strip().split(' ')[1]
        html_description = HtmlResponse(
            url="description", body=response.css("script[type='text/x-template']::text").get().strip(), encoding='utf-8')
        descriptions = html_description.css('p::text').getall()
        description = ''
        for des in descriptions:
            if len(des.split(' ')) != 1:
                description += des + ' '
        html_info = HtmlResponse(url='description', body=response.css(
            "script[type='text/x-template']::text").getall()[1].strip(), encoding='utf-8')
        
        keys = html_info.css('li span.key')
        vals = html_info.css('li span.value')
        for row in zip(keys,vals):
            key = row[0].css('::text').get().strip().lower()
            if 'vermietet' in key:
                if row[1].css('span.fafa-times').get() != None:
                    return
                continue      
            if row[1].css(('::text')).get() == None:
                continue
            val = row[1].css(('::text')).get().strip()
            if 'kaltmiete' in key:
                rent, currency = extract_rent_currency(
                    val, self.country, BaardseGmbhImmobilienSpider)
                rent = get_price(val)
            elif 'nebenkosten' in key:
                utilities = get_price(val)
            elif 'heizkosten' in key:
                heating_cost = get_price(val)
            elif 'warmmiete' in key:
                heating_cost = get_price(val) - rent
            elif 'zimmer:' in key and key[0] == 'z':
                room_count = int(float(val.replace(',', '.')))
            elif 'badezimmer' in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif 'kaution' in key:
                deposit = get_price(val)
            elif 'verfügbar ab' in key:
                if 'vermietet' in val.lower():
                    return
                elif 'rücksprache' in val.lower() or 'nach vereinbarung' in val.lower():
                    available_date = None
                elif 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif 'fahrstuhl' in key:
                elevator = True
            elif 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
        

        if rent is None:
                return
        images = response.css('div.row div.col-24 img::attr(src)').getall()
        landlord_name = 'baardse GmbH'
        landlord_phone = '0221 / 944060-0'
        landlord_email = 'hausverwaltung@baardse-koeln.de'
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
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format
        get_amenities(description, '', item_loader)
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
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()


def get_price(val):
    v = int(float(extract_number_only(
        val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(
        val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price
