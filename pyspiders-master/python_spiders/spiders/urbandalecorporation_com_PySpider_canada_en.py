# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class urbandalecorporation_com_PySpider_canadaSpider(scrapy.Spider):
    name = "urbandalecorporation_com"
    start_urls = ['https://www.urbandalecorporation.com/residential-rentals']
    allowed_domains = ["urbandalecorporation.com"]
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
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&building_ids=17982,17983,17984,17985,17986,17988,23747,24343,24344,24345,24346,24355,24356,24357,24358,24359,24360,25626,26239,26321,27317,27633,32129,166824&client_id=307&auth_token=sswpREkUtyeYjeoahA2i&city_id=2084&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2300&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=condo&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
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
            counter = 2
            anchor = 1
            for i in range(availability_count):
                item_loader = ListingLoader(response=response)
                images = None
                if availability_count > 1:
                    room_count = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-type.cell > span::text').get()
                    bathroom_count = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-bath.cell::text').get()
                    rent = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-rate.cell > span::text').get()
                    square_meters = int(response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-sqft.cell > span.value::text').get())
                    floor_plan_images = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-floorplans.cell > a::attr(href)').get()
                    try:
                        images = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-photos.cell > a::attr(href)').extract()
                    except:
                        pass
                    available_date = response.css('#suites > section > div > div:nth-child('+str(counter)+') > div.suite-wrap > div.suite-availability.cell > a::text').get()
                    counter += 1
                else:
                    room_count = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-type.cell > span::text').get()
                    bathroom_count = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-bath.cell::text').get()
                    rent = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-rate.cell > span::text').get()
                    square_meters = int(response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-sqft.cell > span.value::text').get())
                    floor_plan_images = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-floorplans.cell > a::attr(href)').get()
                    try:
                        images = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-photos.cell > a::attr(href)').extract()
                    except:
                        pass
                    available_date = response.css('#suites > section > div > div.suite.aos-init.aos-animate > div.suite-wrap > div.suite-availability.cell > a::text').get()                    
                
                room_count = int(room_count.split('Bedroom')[0])
                if '.5'in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
                rent = int(rent.replace('$',''))
                square_meters = int(square_meters)
                if available_date == 'Available Now':
                    available_date = None
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
                if 'Townhouse' in property_type:
                    property_type = 'house'
                else:
                    property_type = 'apartment'
                amenities = response.css('.amenity-holder::text').extract()
                dishwasher = None
                balcony = None
                elevator = None
                washing_machine = None
                parking = None
                terrace = None
                swimming_pool = None

                for i in range(len(amenities)):
                    if 'Dishwasher' in amenities[i]:
                        dishwasher = True
                    if 'terrace' in amenities[i]:
                        terrace = True
                    if 'balconies' in amenities[i] or 'Balconies' in amenities[i]:
                        balcony = True
                    if 'Elevators' in amenities[i]:
                        elevator = True
                    if 'pool' in amenities[i]:
                        swimming_pool = True
                    if 'parking' in amenities[i]:
                        parking = True
                    if 'Laundry' in amenities[i]:
                        washing_machine = True
                for i in range(len(images)):
                    if 'virtualModal' in images[i]:
                        images.pop(i)

                if len(images) < 2:
                    images = response.css('#slickslider-default-id-0 .cover').extract()
                    for i in range(len(images)):
                        images[i] = images[i].split('data-src2x="')[1].split('"')[0]

                item_loader.add_value("external_link", response.url.replace(':443','')+f'#{anchor}') # String
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
                item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", landlord_email) # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
