# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class kpmrentals_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "kpmrentals_ca"
    start_urls = ['https://www.kpmrentals.ca/apartments']
    allowed_domains = ["kpmrentals.ca"]
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
        yield Request(url='https://api.theliftsystem.com/v2/search?client_id=287&auth_token=sswpREkUtyeYjeoahA2i&city_id=2081&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=500&max_rate=2500&show_custom_fields=true&region=&keyword=false&property_types=&city_ids=3133%2C2081&ownership_types=&exclude_ownership_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=30&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response, **kwargs):
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
            landlord_name = item['client']['name']
            landlord_phone = item['client']['phone']
            landlord_email = item['client']['email']
            availability = item['availability_status']
            availability_count = item['availability_count']
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
                  'landlord_phone':landlord_phone,
                  'landlord_email':landlord_email,
                  'availability':availability,
                  'availability_count':availability_count
                  })



    def populate_item(self, response):
        
        availability = response.meta.get("availability")
        if availability > 0:
            availability_count = response.meta.get("availability_count")
            counter = 1
            anchor = 1
            for i in range(availability_count):
                item_loader = ListingLoader(response=response)
                square_meters = None
                if availability_count > 1:
                    room_count = response.css('#suite-section > section > div > div:nth-child('+str(counter)+') > div.row-fluid.top-half > div.suite-type::text').get()
                    bathroom_count = response.css('#suite-section > section > div > div:nth-child('+str(counter)+') > div.row-fluid.bottom-half > div.suite-bath.pad-top > span::text').get()
                    rent = response.css('#suite-section > section > div > div:nth-child('+str(counter)+') > div.row-fluid.top-half > div.suite-rate > span.value::text').get()
                    try:
                        square_meters = int(response.css('#suite-section > section > div > div:nth-child('+str(counter)+') > div.row-fluid.bottom-half > div.suite-sqft.pad-top > span::text').get())
                    except:
                        pass
                    counter += 1
                else:
                    room_count = response.css('#suite-section > section > div > div > div.row-fluid.top-half > div.suite-type::text').get()
                    bathroom_count = response.css('#suite-section > section > div > div > div.row-fluid.bottom-half > div.suite-bath.pad-top > span::text').get()
                    rent = response.css('#suite-section > section > div > div > div.row-fluid.top-half > div.suite-rate > span.value::text').get()
                    try:
                        square_meters = int(response.css('#suite-section > section > div > div > div.row-fluid.bottom-half > div.suite-sqft.pad-top > span::text').get())
                    except:
                        pass
                    if square_meters is None:
                        square_meters = int(response.css('#suite-section > section > div > div > div.row-fluid.bottom-half > div.suite-sqft.pad-top > span:nth-child(1)::text').get())
                try:
                    room_count = room_count.lower()
                except:
                    pass
                if 'one' in room_count:
                    room_count = 1
                elif 'two' in room_count:
                    room_count = 2
                elif 'three' in room_count:
                    room_count = 3
                else:
                    room_count = int(room_count.split('bedroom')[0])
                if '.5'in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
                rent = int(rent.replace('$',''))
                square_meters = int(square_meters)
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
                landlord_email = response.meta.get("landlord_email")
                if 'apartment' in property_type:
                    property_type = 'apartment'
                else:
                    property_type = 'house'
                amenities = response.css('.amenities li::text').extract()
                balcony = None
                dishwasher = None
                washing_machine = None
                parking = None
                if 'Balconies' in amenities:
                    balcony = True
                if 'Dishwasher available' in amenities:
                    dishwasher = True
                if 'Laundry facilities' in amenities:
                    washing_machine = True
                if 'Outdoor parking' in amenities or 'Visitor parking' in amenities or 'Covered parking' in amenities:
                    parking = True

                images = response.css('img::attr(src)').extract()

                item_loader.add_value("external_link", response.url+f'#{anchor}') # String
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

                # item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                # item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                # item_loader.add_value("terrace", terrace) # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                item_loader.add_value("landlord_email", landlord_email) # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
