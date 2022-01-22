# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class summitmanagement_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "summitmanagement_ca"
    start_urls = ['https://www.summitmanagement.ca/apartments-for-rent/montreal']
    allowed_domains = ["summitmanagement.ca"]
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
        yield Request(url='https://api.theliftsystem.com/v2/search?client_id=162&auth_token=sswpREkUtyeYjeoahA2i&city_id=1863&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=10000&min_sqft=0&max_sqft=10000&region=&keyword=false&property_types=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=30&neighbourhood=&amenities=&promotions=&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
                    
    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            external_id = item['id']
            title = item['name']
            property_type = item['property_type']
            address = item['address']['address']
            city = item['address']['city']
            zipcode = item['address']['postal_code']
            pets_allowed = item['pet_friendly']
            description = item['details']['overview']
            latitude = item['geocode']['latitude']
            longitude = item['geocode']['longitude']
            landlord_name = item['contact']['name']
            landlord_phone = item['contact']['phone']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':external_id,
                  'title':title,
                  'property_type': property_type,
                  'address': address,
                  'city': city,
                  'zipcode': zipcode,
                  'pets_allowed': pets_allowed,
                  'description': description,
                  'latitude':latitude,
                  'longitude':longitude,
                  'landlord_name':landlord_name,
                  'landlord_phone':landlord_phone})

    
    def populate_item(self, response):
        counter = 2
        suites = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div.suite').extract()
        anchor = 1
        for i in range(len(suites)):
            

            amenities = response.css('.span12 li::text').extract()
            images = response.css('.cover').extract()
            for i in range(len(images)):
                images[i] = images[i].split('data-src2x="')[1].split('"')[0]
            
            
            room_count = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div:nth-child('+str(counter)+') > div.suite-type.box > div::text').get()
            bathroom_count = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div:nth-child('+str(counter)+') > div.suite-bath.box > div > span::text').get()
            square_meterss = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div:nth-child('+str(counter)+') > div.suite-sqft.box > div > span *::text').extract()
            rents = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div:nth-child('+str(counter)+') > div.suite-rate.box > div > span *::text').extract()
            available_date = response.css('#content > div:nth-child(3) > div > div > section.widget-suites > div > div:nth-child('+str(counter)+') > div.suite-availability.box > div > a::text').get()
            counter += 1
            for i in range(len(square_meterss)):
                item_loader = ListingLoader(response=response)
                square_meters = int(square_meterss[i])
                rent = int(rents[i].replace('$',''))
                bathroom_count = int(bathroom_count)
                room_count = str(room_count).lower()
                property_type = 'apartment'
                if 'studio' in room_count:
                    room_count = 1
                    property_type = 'studio'
                elif 'penthouse' in room_count:
                    room_count = 1
                else:
                    room_count = int(room_count.split('bedroom')[0])
                if available_date is not None: 
                    available_date = available_date.lower()
                try:
                    if 'available now' in available_date:
                        available_date = None
                    else:
                        available_date = available_date.strip()
                except:
                    pass
                external_id = str(response.meta.get("external_id"))
                title = response.meta.get("title")
                property_type = response.meta.get("property_type")
                address = response.meta.get("address")
                city = response.meta.get("city")
                zipcode = response.meta.get("zipcode")
                pets_allowed = response.meta.get("pets_allowed")
                description = response.meta.get("description")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")
                landlord_name = response.meta.get("landlord_name")
                landlord_phone = response.meta.get("landlord_phone")

                temp = ''
                for i in range(len(amenities)):
                    temp = temp + ' ' + amenities[i]
                temp = temp.lower()
                balcony = None
                dishwasher = None
                washing_machine = None
                swimming_pool = None
                parking = None
                elevator = None
                if 'balcony' in temp or 'balconies' in temp:
                    balcony = True
                if 'dishwasher' in temp:
                    dishwasher = True
                if 'washer' in temp:
                    washing_machine = True
                if 'pool' in temp:
                    swimming_pool = True
                if 'parking' in temp:
                    parking = True
                if 'elevators' in temp:
                    elevator = True

                
                if 'apartment' in property_type:
                    property_type = 'apartment'
                else:
                    property_type = 'house'

                countera = 0
                while countera in range(len(images)):
                    if '.png' in images[countera]:
                        images.pop(countera)
                        continue
                    countera = countera + 1
                
                item_loader.add_value("external_link", response.url+f"#{anchor}") # String
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

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", 'tristan@summitmanagement.ca') # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
