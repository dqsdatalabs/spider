# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class fidelitypm_com_PySpider_canadaSpider(scrapy.Spider):
    name = "fidelitypm_com"
    start_urls = ['https://www.fidelitypm.com/residential']
    allowed_domains = ["fidelitypm.com"]
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
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&client_id=582&auth_token=sswpREkUtyeYjeoahA2i&city_id=2081&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4000&min_sqft=0&max_sqft=30000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=329%2C1837%2C3133%2C1607%2C2015%2C415%2C2566%2C3212%2C1425%2C2081&pet_friendly=&offset=0&count=false',
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
            landlord_phone = item['contact']['phone']
            landlord_email = item['contact']['email']
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
                  'landlord_phone':landlord_phone,
                  'landlord_email':landlord_email})


    
    def populate_item(self, response):
        anchor = 1
        suites = response.css('.suite-info-container').extract()
        for i in range(len(suites)):
            item_loader = ListingLoader(response=response)
            property_type = response.meta.get("property_type")
            square_meters = None
            try:
                square_meters = suites[i].split('Square feet                         </span>')[1].split('</span>')[0]
                square_meters = int(square_meters.split('>\n')[1])
            except:
                pass
            if 'apartment' in property_type:
                property_type = 'apartment'
            else:
                property_type = 'house'
            room_count = suites[i].split('Bedrooms                 </span>')[1].split('</span>')[0]
            bathroom_count = suites[i].split('Bathrooms</span>')[1].split('</span>')[0]
            rent = None
            try:
                rent = suites[i].split('Rent From                         </span>')[1].split('</span>')[0]
            except:
                pass
            availability = suites[i].split('Availability')[1]
            room_count = int(room_count.split('>')[1])
            property_type = 'apartment'
            if room_count == 0:
                room_count = 1
                property_type = 'studio'
            bathroom_count = bathroom_count.split('>')[1]
            if '.5' in bathroom_count:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            else:
                bathroom_count = int(bathroom_count)
            try:
                rent = rent.replace('$','').strip()
                rent = rent.split('>')[1]
                rent = rent.split('\n')[1]
                if ',' in rent:
                    rent = int(rent.replace(',',''))
                else:
                    rent = int(rent)
            except:
                pass
            external_id = response.meta.get("external_id")
            title = response.meta.get("title")
            address = response.meta.get("address")
            city = response.meta.get("city")
            zipcode = response.meta.get("zipcode")
            pets_allowed = response.meta.get("pets_allowed")
            description = response.meta.get("description")
            latitude = response.meta.get("latitude")
            longitude = response.meta.get("longitude")
            landlord_phone = response.meta.get("landlord_phone")
            landlord_email = response.meta.get("landlord_email")

            amenities = response.css('.amenity-group').extract()
            tempo = ''
            for i in range(len(amenities)):
                tempo = tempo + ' ' + amenities[i]
            tempo = tempo.lower()


            pets_allowed = str(pets_allowed)
            if 'True' in pets_allowed:
                pets_allowed = True
            elif 'False' in pets_allowed:
                pets_allowed = False
            else:
                pets_allowed = None

            balcony = None
            washing_machine = None
            elevator = None
            parking = None
            dishwasher = None
            swimming_pool = None
            if 'balconies' in tempo:
                balcony = True
            if 'laundry' in tempo:
                washing_machine = True
            if 'elevators' in tempo:
                elevator = True
            if 'parking' in tempo:
                parking = True
            if 'dishwasher' in tempo:
                dishwasher = True
            if 'pool' in tempo:
                swimming_pool = True

            if rent == 0:
                rent = None
            images = response.css('.cover').extract()
            for i in range(len(images)):
                images[i] = images[i].split('data-src2x="')[1].split('"')[0]
            if 'please' in description.lower():
                description = description.lower().split('please')[0]
            if 'Call' in description:
                description = description.split('Call')[0]
            if rent is not None:
                item_loader.add_value("external_link", response.url.replace(':443','')+f"#{anchor}") # String
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

                #item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
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
                item_loader.add_value("landlord_name", "Fidelity Property Management Ltd") # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", landlord_email) # String
                anchor += 1
                self.position += 1
                yield item_loader.load_item()
