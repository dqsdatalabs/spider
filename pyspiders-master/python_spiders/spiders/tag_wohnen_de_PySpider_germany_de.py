# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates
import math

class tag_wohnen_de_PySpider_germanySpider(scrapy.Spider):
    name = "tag_wohnen_de"
    start_urls = ['https://tag-wohnen.de/immosuche?size=10&view=LIST']
    allowed_domains = ["tag-wohnen.de"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def start_requests(self):
        counter = 1
        for i in range (0,92):
            yield Request(url= 'https://immo.isp-10130-1.domservice.de/properties?page='+str(counter)+'&size=10&view=LIST',
                        callback=self.parse,
                        body='',
                        method='GET')
            counter += 1

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['response']['results']:
            url = 'https://tag-wohnen.de/immosuche/expose?object_id=' + item['id']
            external_id = item['id']
            title = item['title']
            address = item['address']
            landlord_email = item['email']
            room_count = None
            try:
                room_count = item['number_of_rooms']
            except:
                pass
            square_meters = float(item['living_space'])
            rent = item['overall_warm']
            net_rent = item['netto_cold']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':external_id,
                  'title':title,
                  'address': address,
                  'landlord_email':landlord_email,
                  'room_count':room_count,
                  'square_meters':square_meters,
                  'rent':rent,
                  'net_rent':net_rent,
                  })
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
       
        external_id = response.meta.get("external_id")
        title = response.meta.get("title")
        address = response.meta.get("address")
        landlord_email = response.meta.get("landlord_email")
        room_count = response.meta.get("room_count")
        square_meters = response.meta.get("square_meters")
        net_rent = str(response.meta.get("net_rent"))
        rent = str(response.meta.get("rent"))
        
        if room_count is None:
            room_count = 1
        
        room_count = str(room_count)
        if '.5' in room_count:
            room_count = int(room_count.split('.5')[0])+1
        elif '.0' in room_count:
            room_count = int(room_count.split('.')[0])
        else:
            room_count = int(room_count)
        
        if '.' in net_rent:
            net_rent = int(net_rent.split('.')[0])
        else:
            net_rent = int(net_rent)

        if '.' in rent:
            rent = int(rent.split('.')[0])
        else:
            rent = int(rent)

        utilities = rent - net_rent

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        square_meters = int(math.ceil(square_meters))


        item_loader.add_value("external_link", response.url) # String
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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        # item_loader.add_value("images", images) # Array
        # item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "tag-wohnen") # String
        #item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
