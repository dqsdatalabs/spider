# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class malleumproperties_com_PySpider_canadaSpider(scrapy.Spider):
    name = "malleumproperties_com"
    start_urls = ['https://www.malleumproperties.com/residential?&available_suites_only=1']
    allowed_domains = ["malleumproperties.com"]
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
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&client_id=906&auth_token=sswpREkUtyeYjeoahA2i&city_id=1174&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2200&min_sqft=0&max_sqft=10000&only_available_suites=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            external_id = str(item['id'])
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
            meta={'external_id':external_id,
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
        anchor = 1 
        counter = 2 
        suites = response.css('#suites > section > div > div').extract()
        for i in range(len(suites)-1):
            item_loader = ListingLoader(response=response)
            room_count = None
            try:
                room_count = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-type.cell::text').get()
            except:
                pass
            bathroom_count = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-bath.cell > span.value::text').get()
            rent = None
            try:
                rent = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-rate.cell > span::text').get()
            except:
                pass
            images = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-photos.cell > a::attr(href)').extract()
            floor_plan_images = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-floorplans.cell > div > a::attr(href)').extract()
            counter +=1
            amenities = response.css('.amenity-holder::text').extract()
            temp = ''
            for i in range(len(amenities)):
                temp = temp + ' ' + amenities[i]
            temp = temp.lower()

            try:
                rent = int(rent.replace('$',''))
            except:
                pass

            property_type = 'apartment'
            try:
                if 'Loft' in room_count or 'Studio' in room_count:
                    property_type = 'studio'
                    room_count = 1
                elif 'Penthouse' in room_count:
                    room_count = 1
                else:
                    room_count = int(room_count.lower().split('bedroom')[0])
            except:
                pass
            if room_count is None:
                room_count = 1
            if '.5' in bathroom_count:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            else:
                bathroom_count = int(bathroom_count)

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
            if 'apartment' in property_type:
                property_type = 'apartment'
            else:
                property_type = 'house'
            
            parking = None
            dishwasher = None
            washing_machine = None
            balcony = None
            if 'parking' in temp:
                parking = True
            if 'dishwasher' in temp:
                dishwasher = True
            if 'washer' in temp or 'laundry' in temp:
                washing_machine = True
            if 'balconies' in temp:
                balcony = True
            if rent is not None:
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
                # item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                item_loader.add_value("landlord_name", "Malleum Properties") # String
                item_loader.add_value("landlord_phone", "844-275-3844") # String
                item_loader.add_value("landlord_email", "leasing@malleumproperties.com") # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
