# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class birchhousing_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "birchhousing_ca"
    start_urls = ['http://www.birchhousing.ca/properties']
    allowed_domains = ["birchhousing.ca"]
    country = 'canada'
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def start_requests(self):
        yield Request(url='http://api.theliftsystem.com/v2/search?client_id=477&auth_token=sswpREkUtyeYjeoahA2i&city_id=2042&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1400&max_rate=3000&min_sqft=0&max_sqft=10000&show_all_properties=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=3133%2C1975%2C2081%2C1607%2C2042&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            availability = item['availability_count']
            if availability > 0:
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
                yield Request(url=url, callback=self.populate_item,
                meta={'external_id':str(external_id),
                    'title':title,
                    'property_type': property_type,
                    'address': address,
                    'city': city,
                    'zipcode': zipcode,
                    'pets_allowed': pets_allowed,
                    'description': description,
                    'latitude':latitude,
                    'longitude':longitude})

    
    def populate_item(self, response):
        suites = response.css('#content > section > div > section.widget.suites > div.suites > div').extract()
        anchor = 1
        counter = 1
        for i in range(len(suites)):
            item_loader = ListingLoader(response=response)
            images = response.css('#slickslider-default-id-0 .cover').extract()
            for i in range(len(images)):
                images[i] = images[i].split('data-src2x="')[1].split('"')[0]
            available = response.css('#content > section > div > section.widget.suites > div.suites > div:nth-child('+str(counter)+') > div > div > div.suite-availability.cell > p::text').get()
            if 'Available Now' in available:    
                rent = response.css('#content > section > div > section.widget.suites > div.suites > div:nth-child('+str(counter)+') > div > div > div.suite-rate.cell > div::text').get()
                room_count = response.css('#content > section > div > section.widget.suites > div.suites > div:nth-child('+str(counter)+') > div > div > div.suite-type.cell > div::text').get()
                try:
                    images = response.css('#content > section > div > section.widget.suites > div.suites > div:nth-child('+str(counter)+') > div > div > div.suite-photos.cell > div > a::attr(href)').extract()
                except:
                    pass
                 

                room_count = int(room_count.split('Bedroom')[0])
                rent = int(rent.split('$')[1])
                
                external_id = response.meta.get("external_id")
                title = response.meta.get("title")
                property_type = response.meta.get("property_type")
                address = response.meta.get("address")
                city = response.meta.get("city")
                zipcode = response.meta.get("zipcode")
                pets_allowed = response.meta.get("pets_allowed")
                description = response.meta.get("description")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")

                pets_allowed = str(pets_allowed)
                if 'True' in pets_allowed:
                    pets_allowed = True
                elif 'False' in pets_allowed:
                    pets_allowed = False
                else:
                    pets_allowed = None

                description = response.css('#content > div:nth-child(2) > div > div > div > p::text').get()

                if 'apartment' in property_type:
                    property_type = 'apartment'
                else:
                    property_type = 'house'

                amenities = response.css('.amenity::text').extract()
                tempo = ''
                for i in range(len(amenities)):
                    tempo = tempo + ' ' + amenities[i]
                tempo = tempo.lower()

                balcony = None
                washing_machine = None
                elevator = None
                parking = None
                dishwasher = None
                swimming_pool = None
                if 'balconies' in tempo:
                    balcony = True
                if 'laundry' in tempo or 'washer' in tempo:
                    washing_machine = True
                if 'elevators' in tempo:
                    elevator = True
                if 'parking' in tempo:
                    parking = True
                if 'dishwasher' in tempo:
                    dishwasher = True
                if 'pool' in tempo:
                    swimming_pool = True

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
                #item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                #item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

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
                item_loader.add_value("landlord_name", "birchhousing") # String
                item_loader.add_value("landlord_phone", "(416) 283-3939") # String
                item_loader.add_value("landlord_email", "info@birchhousing.ca") # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
            counter += 1
