# -*- coding: utf-8 -*-
# Author: Muhammad Alaa 
import scrapy
from ..loaders import ListingLoader
from ..helper import description_cleaner, extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, get_amenities, get_price
from dateutil.parser import parse
from datetime import datetime


class VermietungshalberSpider(scrapy.Spider):
    name = "Vermietungshalber"
    start_urls = ['https://www.vonrodenhausen.de/aktuelle-angebote']
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
        pages = response.css('ul.pagination a::attr(href)').getall()
        for page in pages:
            yield scrapy.Request(url='https://www.vonrodenhausen.de' + page, callback=self.parse_pages)


    def parse_pages(self, response, **kwargs):
        properties = response.css('div.immomain_list.load_more_list a::attr(href)').getall()
        for prop in properties:
            yield scrapy.Request('https://www.vonrodenhausen.de' + prop, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = balcony = terrace = elevator = external_id = furnished = parking = None
        bathroom_count = available_date = deposit = heating_cost = rent = currency = square_meters = None
        room_count = 1
        keys = response.css('div.question_immoinfo_content ul strong::text').getall()
        vals = response.css('div.question_immoinfo_content ul li::text').getall()
        images = response.css('div.gallery_slider_move img::attr(src)').getall()
        for image_index in range(len(images)):
            images[image_index] = 'https://www.vonrodenhausen.de' + images[image_index]
        property_type = 'apartment'
        prop = response.css('div.question_immoinfo h2::text').get()
        if prop.find('haus') != -1:
            property_type = 'house'
        elif prop.find('wohnung') == -1:
            return
        prob_cond = response.css('div.sold::text').get()
        if prob_cond is not None:
            if 'vermietet' in prob_cond or 'verkauft' in prob_cond:
                return
        
        description = response.css('div.question_text_block p::text').get().split('Virtueller')[0].strip() + ' ' + response.css('div.question_text_block p::text').getall()[1].split('Virtueller')[0].strip()
        description += ' ' + response.css('div.question_text_block p::text').getall()[2].split('Virtueller')[0].strip()
        if 'Kaltmiete' not in description:
            return
        rent = None
        for row in zip(keys, vals):
            key = row[0].strip().lower()
            val = row[1].strip()
            if 'kaltmiete' in key:
                rent, currency = extract_rent_currency(
                    val, self.country, VermietungshalberSpider)
                rent = get_price(val)
            elif 'wohnfläche' in key:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator='.', scale_separator=',')))
            elif 'zimmer' in key and key[0] == 'z':
                room_count = int(float(val.replace(',', '.')))
            elif 'badezimmer' in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif 'terrassen' in key:
                terrace = True
        if rent == None:
            after_equal = description.split('=')
            if len(after_equal) > 1:
                rent = get_price(after_equal[1].split('Kaltmiete')[0].strip())
            else:
                before_rent = description.split('Kaltmiete pro Monat')[0]
                rent = before_rent.split('.')[-1].strip()
        if rent == None:
            return
        address = response.css('div.question_immoinfo_adresse ul li::text').get()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = response.css('div.intro_default_headlines.intro_default_headlines_immo h1::text').get()
        landlord_name = 'Rodenhausen Immobilien'
        landlord_number = '+49 89 215 400 590'
        landlord_email = 'info@vonrodenhausen.de'

        item_loader = ListingLoader(response=response)


        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
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

        #item_loader.add_value("available_date", available_date) # String => date_format

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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", 'EUR') # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
