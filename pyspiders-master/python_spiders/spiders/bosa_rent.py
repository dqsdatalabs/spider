# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *

class BosaRentSpider(scrapy.Spider):
    name = "bosa_rent"
    start_urls = ['https://bosa4rent.com/en/find-a-suite/']
    allowed_domains = ["bosa4rent.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        property_urls = response.css('.tile__btn::attr(href)').extract()
        property_urls = ['https://bosa4rent.com/' + x for x in property_urls]
        types = response.css('.tile__title-item:nth-child(1)::text').extract()
        for index, property_url in enumerate(property_urls):
            yield Request(url=property_url, callback=self.populate_item, meta={'type': types[index]})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        description = response.css(".amenties__rte li::text , .amenties__rte p::text").extract()
        description = ' '.join(description)
        property_type = response.meta['type']
        if 'Studio' in property_type:
            room_count = 1
            property_type = 'studio'
        else:
            room_count = int(property_type[0])
            property_type = 'room'

        rent = response.css(".amenities__title+ .row .icon:nth-child(4) p::text")[0].extract()
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        square_meters = response.css(".amenities__title+ .row .icon:nth-child(3) p::text")[0].extract()
        square_meters = int(square_meters[:-3])
        bathroom_count = response.css(".amenities__title+ .row .icon:nth-child(1) p::text")[0].extract()
        bathroom_count = int(bathroom_count[0])

        pets_allowed = None
        try:
            pets_allowed = response.css(".amenities__title+ .row .icon:nth-child(2) p span::text")[0].extract()
            if 'Pet Friendly' in pets_allowed:
                pets_allowed = True
        except:
            pass
        list = response.css(".amenties__rte li::text").extract()
        list = ' '.join(list)
        parking = None
        try:
            if 'Parking available' in list:
                parking = True
        except:
            pass
        washing_machine = None
        try:
            if 'laundry room' in list or 'washer' in list:
                washing_machine = True
        except:
            pass
        terrace = None
        try:
            if 'terrace' in list:
                terrace = True
        except:
            pass
        dishwasher = None
        try:
            if 'dishwasher' in list:
                dishwasher = True
        except:
            pass
        latlng = response.css("div.neighbourhood__map.neighbourhood__map > div")[0].extract()
        coords = latlng.split('data-lat="')[1].split('" data-title="')[0]
        coords = coords.split('" data-long="')
        latitude = coords[0]
        longitude = coords[1]
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)

        images = response.css(".image-wrapper img::attr(data-src)").extract()

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        #item_loader.add_value("title", title) # String
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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Bosa4Rent") # String
        item_loader.add_value("landlord_phone", "604 897 3333") # String
        item_loader.add_value("landlord_email", "info@bosa4rent.com") # String

        self.position += 1
        yield item_loader.load_item()
