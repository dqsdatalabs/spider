# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
import json
from ..helper import *


class RentwithklausCaSpider(scrapy.Spider):
    name = "rentwithklaus_ca"
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'http://api.theliftsystem.com/v2/search?locale=en&client_id=815&auth_token=sswpREkUtyeYjeoahA2i&city_id=2699&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6000&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments,+houses,+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=1000&neighbourhood=&amenities=&promotions=&city_ids=3201,737,2238,2013,1715,649,3343,1969,3133,388,2699&pet_friendly=&offset=0&count=false'
        yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):


        props = json.loads(response.text)
        for prop in props:
            if prop["availability_status"] == 1:
                item_loader = ListingLoader(response=response)
                # Property Type
                prop_type = ["apartment", "house", "room", "student_apartment", "studio"]
                property_type = None
                if "student_apartment" in prop['property_type'].lower():
                    property_type = prop_type[3]
                elif "house" in prop['property_type'].lower():
                    property_type = prop_type[1]
                elif "room" in prop['property_type'].lower():
                    property_type = prop_type[2]
                elif "apartment" in prop['property_type'].lower():
                    property_type = prop_type[0]
                elif "studio" in prop['property_type'].lower():
                    property_type = prop_type[4]

                # Latitude, longitude
                latitude = prop['geocode']['latitude']
                longitude = prop['geocode']['longitude']

                # zipcode, city, address
                zipcode, city, address= extract_location_from_coordinates(longitude, latitude)

                # Description
                description = None
                if prop['building_header'] != '':
                    description = prop['building_header']
                if prop['details']['overview'] != '':
                    description = prop['details']['overview']

                # Available date
                available_date = None
                if prop['min_availability_date'] != '':
                    available_date = prop['min_availability_date']

                # Furnished
                furnished = None
                if 'furnished' in prop['building_header'].lower():
                    furnished = True
                if 'unfurnished' in prop['building_header'].lower():
                    furnished = False

                # Pets allowed
                pets_allowed = None
                if prop['pet_friendly'] is True:
                    pets_allowed = True
                if prop['pet_friendly'] is False:
                    pets_allowed = False

                # Parking
                parking = None
                if 'parking' in description.lower():
                    parking = True

                # Balcony
                balcony = None
                if 'balcony' in description.lower():
                    balcony = True

                # terrace
                terrace = None
                if 'terrace' in description.lower():
                    terrace = True

                # Pool
                swimming_pool = None
                if 'pool' in description.lower():
                    swimming_pool = True

                # Washing_machine
                washing_machine = None
                if 'washer' in description.lower():
                    washing_machine = True

                # dishwasher
                dishwasher = None
                if 'dishwasher' in description.lower():
                    dishwasher = True


                # # MetaData
                item_loader.add_value("external_link", prop['permalink'])# String
                item_loader.add_value("external_source", "Rentwithklaus_ca_PySpider_canada_en") # String

                item_loader.add_value("external_id", str(prop["id"])) # String
                item_loader.add_value("position", self.position) # Int
                item_loader.add_value("title", prop['name']) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", int(prop['statistics']['suites']['square_feet']['average']))# Int
                item_loader.add_value("room_count", int(float(prop['statistics']['suites']['bedrooms']['average'])))# Int
                item_loader.add_value("bathroom_count", int(float(prop['statistics']['suites']['bathrooms']['average'])))# Int

                item_loader.add_value("available_date", available_date) # String => date_format
                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", [prop['photo_path']]) # Array
                item_loader.add_value("external_images_count", len([prop['photo_path']]))# Int
                #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", int(prop['statistics']['suites']['rates']['average']))# Int
                #item_loader.add_value("deposit", deposit) # Int
                #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                #item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", prop['contact']['name']) # String
                item_loader.add_value("landlord_phone", prop['contact']['phone']) # String
                item_loader.add_value("landlord_email", prop['contact']['email']) # String

                self.position += 1
                yield item_loader.load_item()
