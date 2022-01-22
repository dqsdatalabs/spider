# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ZgImmobilienDeSpider(scrapy.Spider):
    name = "zg_immobilien_de"
    start_urls = ['https://www.zg-immobilien.de/mieten/wohnungen']
    allowed_domains = ["zg-immobilien.de"]
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
        for url in ['https://www.zg-immobilien.de'+i for i in response.css('h4 a::attr(href)').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1::text').get()
        images = [i for i in response.css('a::attr(href)').getall() if '.jpg' in i]

        landlord_email = 'info@zg-immobilien.de'
        landlord_number = '(08131) 45 45 28'
        landlord_name = 'Dominik Wittman'

        info = dict(zip(response.css('.row:nth-child(5) .col-md-6:nth-child(1) .name::text').extract(), response.css('.row:nth-child(5) .col-md-6:nth-child(1) .value::text').extract()))

        bathroom_count = room_count = 1
        property_type = 'apartment'
        floor = availability_date = heat_cost = square_meters = energy_label = deposit = None
        rent = 0
        warm_rent = 0
        for i in info.keys():
            if 'Objektnummer' in i:
                external_id = info[i]
            if 'Zimmer' in i:
                room_count = int(float(info[i].replace('.', '').replace(',', '.')))
            if 'Warmmiete' in i:
                warm_rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Nebenkosten'.lower() in i.lower():
                heat_cost = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Wohnfläche' in i:
                square_meters = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Badezimmer' in i:
                bathroom_count = int(float(info[i].replace('.', '').replace(',', '.')))
        rent = warm_rent - heat_cost
        description = response.css('.bsdimmo-freitext div:nth-child(2)::text').get()
        details = response.css('.col-md-8::text , .bsdimmo-freitext div:nth-child(8)::text , .bsdimmo-freitext div:nth-child(6)::text').extract()

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, " ".join(details), item_loader)

        description = description_cleaner(description)

        loc = response.css('p:nth-child(2)::text').getall()[1].strip()
        longitude, latitude = extract_location_from_address('Schleißheimer Str. 6 ' + loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if 0 >= int(rent) > 40000:
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heat_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
