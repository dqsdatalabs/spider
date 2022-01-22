# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from dateutil.parser import parse
from datetime import datetime
from ..helper import *

class GebagDuisburgerBaugesellschaftMbhSpider(scrapy.Spider):
    name = "GEBAG_Duisburger_Baugesellschaft_mbH"
    start_urls = ['https://www.gebag.de/mieten/wohnungen-zur-miete']
    allowed_domains = ["gebag.de"]
    country = 'germany'
    locale = 'de'
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
        urls = response.css('div.immoitem a::attr(href)').getall()
        for url in urls:
            yield scrapy.Request('https://www.gebag.de' + url, callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = heating_cost = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'

        keys = response.css("td:nth-child(1)::text").getall()
        vals = response.css("td:nth-child(2)::text").getall()
        for row in zip(keys, vals):
            key = row[0].strip()
            val = row[1].strip()
            key = key.lower()
            if 'objektnummer' in key:
                external_id = val
            elif 'badezimmer' in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif 'zimmer' in key:
                room_count = int(float(val.replace(',', '.')))
            elif 'ort' in key:
                address = val
            elif 'straße' in key:
                street = val
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator='.', scale_separator=',')))
            elif 'bezugstermin' in key:
                available_date = datetime.now().strftime("%Y-%m-%d")
            elif 'balkon' in key:
                if 'Ja' in val.lower():
                    balcony = True
                else: 
                    balcony = False
            elif 'haustiere' in key:
                if 'verhandelbar' in val.lower():
                    pets_allowed = True
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "kaution" in key:
                deposit = get_price(val)
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(
                    val, self.country, GebagDuisburgerBaugesellschaftMbhSpider)
                rent = get_price(val)
            elif "nebenkosten" in key:
                utilities = get_price(val)
            elif "heizkosten" in key:
                heating_cost = get_price(val)
                utilities -= heating_cost
    
        description = ' '.join(response.css(
            'div.expose-text p::text').getall()).strip()
        title = response.css('div.expose.hero-caption h1::text').get()
        if address is None:
            address = response.css('div.expose-info h5').get().replace(
                '<h5>', '').replace('<br>', '').replace('</h5>', '').strip().split(',')
            for index in range(len(address)):
                address[index] = address[index].strip()
            address = ' '.joins(address)
        else:
            address = street + ' ' + address
        amenties = ''.join(response.css(
            'div.expose-features::text').getall()).strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude=longitude, latitude=latitude)
        images = response.css('div.carousel-hero.expose img::attr(src)').getall()
        for img in range(len(images)):
            images[img] = '	https://www.gebag.de' + images[img]
        item_loader = ListingLoader(response=response)
        landlord_name = response.css('div.contact-info p strong::text').get()
        landlord_info = response.css('div.contact-info p a::text').getall()
        landlord_number = landlord_info[0]
        landlord_email = landlord_info[1]

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        get_amenities(description, amenties, item_loader)

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
