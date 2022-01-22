# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import format_date, sq_feet_to_meters, extract_number_only, format_date
from datetime import datetime
import json
import re


class MacmanagementpropertiesincSpider(scrapy.Spider):
    name = "MacManagementPropertiesInc"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=1000&auth_token=sswpREkUtyeYjeoahA2i&city_id=2136&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2900&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3107%2C1978%2C3303%2C1979%2C2870%2C2136&pet_friendly=&offset=0&count=false']
    allowed_domains = ["macmanagement.ca", "theliftsystem.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        listings = json.loads(response.body)
        for listing in listings:
            if listing['availability_status']:
                yield scrapy.Request(listing['permalink'], callback=self.populate_item, meta={ **listing })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        amenities = response.css("div.amenity-holder::text").getall()
        description = '\n'.join([x for x in re.sub(re.compile('<.*?>'), '', data['details']['overview']).split("\n") if '289' not in x and '@' not in x and x != '' ])
        amenities.append(description)
        for index, unit in enumerate(response.css("script[type='application/ld+json']::text").getall()):
            row = response.css("div.suite-wrap")[index]
            unit = json.loads(unit)
            
            external_id = str(unit['sku'])
            title = row.css("div.suite-type::text").get()
            city = data['address']['city']
            zipcode = data['address']['postal_code']
            address = data['address']['address']
            latitude = data['geocode']['latitude']
            longitude = data['geocode']['longitude']
            if index < len(data['matched_beds']):
                room_count = data['matched_beds'][index]
            else:
                room_count = data['matched_beds'][-1]
            if room_count == 0:
                room_count = 1
            property_type = 'apartment'
            if 'room' in data['property_type'].lower():
                property_type = 'room'
            elif 'house' in data['property_type'].lower():
                property_type = 'house'
            elif 'home' in data['property_type'].lower():
                property_type = 'house'
            elif 'apartment' in data['property_type'].lower():
                property_type = 'house'
                
            pets_allowed = data['pet_friendly']
            bathroom_count = int(float(row.css("div.suite-bath.cell > span.value::text").get()))
            if 'Not Available' in row.css("div.suite-availability.cell > *").get():
                continue
            
            parking = None
            balcony = None
            dishwasher = None
            washing_machine = None
            for amenity in amenities:
                if 'balcon' in amenity.lower():
                    balcony = True
                if 'dishwasher' in amenity.lower():
                    dishwasher = True
                if 'parking' in amenity.lower():
                    parking = True
                if 'landry' in amenity.lower():
                    washing_machine = True
            images = row.css("a.suite-photo::attr(href)").getall()
            if len(images) == 0:
                images = [data['photo_path']]
                
            rent = int(float(extract_number_only(row.css("div.suite-rate span::text").get())))
            landlord_name = data['client']['name']
            landlord_number = data['client']['phone']
            landlord_email = data['client']['email']
            
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url + f'#{index+1}') # String
            item_loader.add_value("external_source", self.external_source) # String
            item_loader.add_value("position", self.position) # Int

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

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
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
