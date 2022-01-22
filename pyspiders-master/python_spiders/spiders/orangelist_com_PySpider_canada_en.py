# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class orangelist_com_PySpider_canadaSpider(scrapy.Spider):
    name = "orangelist_com"
    start_urls = ['http://www.$domain/']
    allowed_domains = ["orangelist.com"]
    country = 'Canada'
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def start_requests(self):
        yield Request(url='https://www.orangelist.com/listings/_search',
                    callback=self.parse,
                    body='',
                    method='POST')
    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['list']:
            external_id = item['id']
            url = 'https://www.orangelist.com/listings/view/id/' + external_id
            title = item['title']
            address = item['address']
            city = item['city']
            zipcode = item['postal_code']
            latitude = item['latitude']
            longitude = item['longitude']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':str(external_id),
                  'title':title,
                  'address': address,
                  'city': city,
                  'zipcode': zipcode,
                  'latitude':latitude,
                  'longitude':longitude})

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.meta.get("external_id")
        title = response.meta.get("title")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcode")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        
        description = response.css('#content-container > div > div.row > div.col-sm-7 *::text').extract()
        temp = ''
        for i in range(len(description)):
            temp = temp + ' ' + description[i]
        description = temp
        temp = temp.lower()
    
        description = description.split(' Description ')[1]
        description = description.split('Please connect')[0]

        furnished = None
        washing_machine = None
        parking = None
        if 'furnished: no' in description.lower():
            furnished = False
        elif 'furnished: yes' in description.lower():
            furnished = True

        if 'laundry is not available' in description.lower():
            washing_machine = False
        elif 'laundry' in description.lower():
            washing_machine = True
        
        if 'parking' in description.lower():
            parking = True

        

        images = response.css('#lightSlider img::attr(src)').extract()
        for i in range(len(images)):
            images[i] = 'https://www.orangelist.com' + images[i]
    
        room_count = int(response.css('#content-container > div > div.row > div.col-sm-5.property-info-container > p:nth-child(11)::text').get())
        bathroom_count = int(response.css('#content-container > div > div.row > div.col-sm-5.property-info-container > p:nth-child(12)::text').get())
        rent = response.css('#content-container > div > div.row > div.col-sm-5.property-info-container > p.heading.three.price::text').get()
        rent = int(rent.replace('$','').split('.')[0])
        
        property_type = response.css('#content-container > div > div.row > div.col-sm-5.property-info-container > p:nth-child(10)::text').get()
        if 'room' in property_type.lower():
            property_type = 'studio'
            room_count = 1
        elif 'apartment' in property_type.lower():
            property_type = 'apartment'
        else:
            property_type = 'house'

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
        #item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "OrangeList") # String
        item_loader.add_value("landlord_phone", "(905) 329-0823") # String
        item_loader.add_value("landlord_email", "manager@orangelist.com") # String

        self.position += 1
        yield item_loader.load_item()
