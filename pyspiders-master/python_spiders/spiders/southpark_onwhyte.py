# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
import json
from ..helper import *


class SouthparkOnwhyteSpider(scrapy.Spider):
    name = "southpark_onwhyte"
    start_urls = ['http://www.southparkonwhyte.com/floorplans']
    allowed_domains = ["southparkonwhyte.com"]
    country = 'canada'
    locale = 'en'
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
        script = response.css('script:contains("var pageData")::text').get()
        script = script.split('floorplans: ')[1].split('propertyID: 1310329,')[0]
        index = 0
        ids = []
        while index < len(script):
            index = script.find('id:', index)
            if index == -1:
                break
            ids.append(script[index + 4:index + 11])
            index += 3
        index = 0
        sqft = []
        while index < len(script):
            index = script.find('sqft:', index)
            if index == -1:
                break
            sqft.append(script[index + 7:index + 12])
            index += 5
        index = 0
        beds = []
        while index < len(script):
            index = script.find('beds:', index)
            if index == -1:
                break
            beds.append(script[index + 6:index + 7])
            index += 5
        index = 0
        baths = []
        while index < len(script):
            index = script.find('baths:', index)
            if index == -1:
                break
            baths.append(script[index + 7:index + 8])
            index += 6
        property_urls = ['https://www.southparkonwhyte.com/availableunits.aspx?myOlePropertyId=1221116&floorPlans=' + x
                         for x in ids]
        for index, property_url in enumerate(property_urls):
            yield Request(url=property_url,
                          callback=self.populate_item,
                          meta={'sqft': sqft[index],
                                'bed': beds[index],
                                'bath': baths[index]})
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        square_meters = response.meta['sqft']
        if any(char.isdigit() for char in square_meters):
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        room_count = int(response.meta['bed'])
        bathroom_count = int(response.meta['bath'])

        skip = response.css(".m-t-sm").extract()
        skip = response.css(".m-t-sm").extract()
        if skip == []:
            pass
        else:
            return

        title = response.css("h3::text")[0].extract()
        title = title.split('Floor Plan : ')[1].split(' - ')[0]

        floor_plan_images = response.css("#links a::attr(onmouseout)")[0].extract()
        floor_plan_images = floor_plan_images.split(",this,false,'")[1].split("?width=350','left'")[0]
        if ",w_350" in floor_plan_images:
            floor_plan_images = [floor_plan_images.replace(',w_350', '')]
        floor_plan_images = ['https://cdngeneral.rentcafe.com/' +  floor_plan_images]

        address = '8122 106 ST NW Edmonton, AB T6E 3S3'
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        list = response.css("td::text").extract()
        count = int(len(list) / 3)
        for i in range(count):
            item_loader = ListingLoader(response=response)
            external_link = response.url + '#' + str(i + 1)
            external_id = list[i * 3]
            rent = list[(i * 3) + 2]
            if any(char.isdigit() for char in rent):
                rent = int(''.join(x for x in rent if x.isdigit()))

            # # MetaData
            item_loader.add_value("external_link", external_link) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            #item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

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
            #item_loader.add_value("images", images) # Array
            #item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", "SouthPark On Whyte") # String
            item_loader.add_value("landlord_phone", "833 841 0328") # String
            item_loader.add_value("landlord_email", "info@SouthparkOnWhyte") # String

            self.position += 1
            yield item_loader.load_item()
