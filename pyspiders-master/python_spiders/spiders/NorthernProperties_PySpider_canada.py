# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
import json


class NorthernpropertiesPyspiderCanadaSpider(scrapy.Spider):
    name = "NorthernProperties"
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=500&auth_token=sswpREkUtyeYjeoahA2i&city_id=547&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=900&max_rate=1700&min_sqft=0&max_sqft=10000&show_all_properties=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false']
    allowed_domains = ["northernprop.ca"]
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
        apartments_json = json.loads(response.text)
        for apartment_json in apartments_json:
            external_id = apartment_json['id']
            url = apartment_json['permalink']
            building_name = apartment_json['name']
            property_type = apartment_json['property_type']
            landlord_name = apartment_json['contact']['name']
            landlord_email = apartment_json['contact']['email']
            landlord_number = apartment_json['contact']['phone']
            address = apartment_json['address']['address']
            city = apartment_json['address']['city']
            zipcode = apartment_json['address']['postal_code']
            pets_allowed = apartment_json['pet_friendly']
            description = apartment_json['details']['overview']
            longitude = apartment_json['geocode']['longitude']
            latitude = apartment_json['geocode']['latitude']
            suites = apartment_json['matched_suite_names']
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'external_id': external_id,
                'building_name': building_name,
                'property_type': property_type,
                'landlord_name': landlord_name,
                'landlord_email': landlord_email,
                'landlord_number': landlord_number,
                'address': address,
                'city': city,
                'zipcode': zipcode,
                'pets_allowed': pets_allowed,
                'description': description,
                'longitude': longitude,
                'latitude': latitude,
                'suites': suites,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        external_id = response.meta.get('external_id')
        external_id = str(external_id)

        building_name = response.meta.get('building_name')

        landlord_name = response.meta.get('landlord_name')
        landlord_email = response.meta.get('landlord_email')
        landlord_number = response.meta.get('landlord_number')

        address = response.meta.get('address')
        city = response.meta.get('city')
        zipcode = response.meta.get('zipcode')
        address = address + ', ' + zipcode + ' ' + city
        longitude = response.meta.get('longitude')
        latitude = response.meta.get('latitude')

        pets_allowed = response.meta.get('pets_allowed')
        if pets_allowed == 'True':
            pets_allowed = True
        else:
            pets_allowed = None

        description = response.meta.get('description')
        description = description.replace('\n', '')
        description = description.replace('\r', '')

        suites = response.css('div.suites div.suite div div.table')
        for suite in suites:
            item_loader = ListingLoader(response=response)

            suite_name = suite.css('div.suite-type div::text')[0].extract()
            suite_name = suite_name.strip()
            suite_name = suite_name.replace('\n', '')

            title = building_name + ' | ' + suite_name

            suite_url = suite_name.replace(' ', '-')
            external_link = response.url + '#' + suite_url

            rent = suite.css('div.suite-rate div::text').extract()
            rent = rent[0].strip()
            rent = rent.replace('\n', '')
            rent = rent.replace('$', '')
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = round(float(rent))
            rent = int(rent)
            # Enforces rent between 0 and 40,000 please dont delete these lines
            if int(rent) <= 0 and int(rent) > 40000:
                return

            images = response.css('div.gallery-image a::attr(href)').extract()

            amenities_one = response.css('div.suite-amenities ul li span::text').extract()
            amenities_two = response.css('div.building-amenities ul li span::text').extract()
            amenities = amenities_two + amenities_one
            balcony = None
            parking = None
            furnished = None
            washing_machine = None
            for amenity in amenities:
                if 'balcon' in amenity.lower():
                    balcony = True
                if 'parking' in amenity.lower():
                    parking = True
                if 'washer' in amenity.lower():
                    washing_machine = True

            room_count = suite_name.split()[0]
            room_count = int(room_count)

            if 'furnished' in suite_name.lower():
                furnished = True

            property_type = 'apartment'

            # # MetaData
            item_loader.add_value("external_link", external_link) # String
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
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
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
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
