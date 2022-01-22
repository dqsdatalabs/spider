# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates
import math


class pomm_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "pomm_ca"
    start_urls = ['https://www.pomm.ca/property-listings/?fwp_paged=1']
    allowed_domains = ["pomm.ca"]
    country = 'Canada'
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    page_number = 2
    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        urls = response.css("#main > div.facetwp-template > div > div > div > div.property-thumbnail > div > a::attr(href)").extract()
        for i in range(len(urls)):
            yield Request(url = urls[i],
            callback=self.populate_item)
        next_page = ("https://www.pomm.ca/property-listings/?fwp_paged="+ str(pomm_ca_PySpider_canadaSpider.page_number))
        if pomm_ca_PySpider_canadaSpider.page_number <= 2:
            pomm_ca_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        

        title = response.css('#main-details > div > div > div.col-md-4 > div > div.property-title > h1::text').get()
        address = title
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        
        
        description = response.css('#main-details > div > div > div.col-md-8 > div.property-description > p *::text').extract()
        temp = ''
        for i in range(len(description)):
            temp = temp + ' ' + description[i]
        description = temp
        if 'Please email' in description:
            description = description.split('Please email')[0]

        room_count = 0
        bathroom_count = 0
        
        
        data = response.css('#property-ammenities > div > div > div > div> span.ammenity-title::text').extract()
        data_value = response.css('#property-ammenities > div > div > div > div > span.ammenity-value::text').extract()

        pets_allowed = None
        dishwasher = None
        parking = None
        washing_machine = None 
        rent = None
        for i in range(len(data)):
            data[i] = data[i].lower()
            if 'price' in data[i]:
                rent = data_value[i]
            if 'bedrooms' in data[i]:
                room_count = int(data_value[i])
            if 'bathrooms' in data[i]:
                bathroom_count = data_value[i]
                if '.5' in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
            if 'pets' in data[i]:
                pets_allowed = data_value[i]
            if 'dishwasher' in data[i]:
                dishwasher = data_value[i]
            if 'parking' in data[i]:
                parking = data_value[i]
            if 'laundry' in data[i]:
                washing_machine = data_value[i]
        property_type = 'apartment'
        if bathroom_count == 0:
            bathroom_count = None
        if room_count ==0:
            room_count = 1 
            property_type = 'studio'

        images = response.css('a::attr(href)').extract()
        tempp = []
        for i in range(len(images)):
            if '.jpg' in images[i]:
                tempp.append(images[i])
        images = tempp

        try:
            if 'No' in pets_allowed:
                pets_allowed = False
            else:
                pets_allowed = True
        except:
            pass

        try:
            if 'Not Included' in washing_machine:
                washing_machine = False
            else:
                washing_machine = True
        except:
            pass

        try:
            if 'Not Included' in dishwasher:
                dishwasher = False
            else:
                dishwasher = True
        except:
            pass

        try:
            if 'Not Included' in parking:
                parking = False
            else:
                parking = True
        except:
            pass

        if rent is not None:
            if 'please contact' not in rent:
                rent = int(rent)
                item_loader.add_value("external_link", response.url) # String
                item_loader.add_value("external_source", self.external_source) # String

                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position) # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", str(latitude)) # String
                item_loader.add_value("longitude", str(longitude)) # String
                #item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                #item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                #item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
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
                item_loader.add_value("landlord_name", "PEACE OF MIND MANAGEMENT") # String
                item_loader.add_value("landlord_phone", "(506) 672-1094") # String
                item_loader.add_value("landlord_email", "peaceofmind@pomm.ca") # String

                self.position += 1
                yield item_loader.load_item()
