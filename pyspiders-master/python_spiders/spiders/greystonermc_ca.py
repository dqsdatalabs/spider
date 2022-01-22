# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, remove_white_spaces, \
    extract_location_from_address
from ..loaders import ListingLoader


class GreystonermcCaSpider(scrapy.Spider):
    name = 'greystonermc_ca'
    allowed_domains = ['greystonermc.ca','api.theliftsystem.com']
    start_urls = ['https://www.greystonermc.ca/']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1
    keywords = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish'],
        'parking': ['parking', 'garage'],
        'elevator': ['elevator'],
        'balcony': ['balcon'],
        'terrace': ['terrace'],
        'swimming_pool': ['pool', 'swim'],
        'washing_machine': ['washing', ' washer', 'laundry'],
        'dishwasher': ['dishwasher']
    }

    def parse(self, response):
        data_city_id = response.css('.autocomplete::attr(data-cities)').extract_first()
        data_city_id = data_city_id.split(',')
        for city in data_city_id:
            api_link = f'https://api.theliftsystem.com/v2/search?locale=en&client_id=661&auth_token=sswpREkUtyeYjeoahA2i&city_id={city}&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4100&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false'
            yield Request(url=api_link,callback=self.apiparse)

    def apiparse(self, response):
        parsed_response = json.loads(response.body)
        for building in parsed_response:
            if int(building['availability_count']) >0:
                if building['parking']['additional'] == "" and building['parking']['indoor'] == "" and building['parking']['outdoor'] == "":
                    parking = False
                else:
                    parking = True
                external_link = building['permalink']
                external_id = building['id']
                property_type = building['property_type']
                if 'home' in property_type or 'house' in property_type:
                    property_type = 'house'
                else:
                    property_type = 'apartment'
                pets_allowed = building['pet_friendly']
                if pets_allowed == 'n/a':
                    pets_allowed = None
                square_meters = int(ceil(float(building['statistics']['suites']['square_feet']['average'])))
                if square_meters == 0:
                    square_meters = None
                landlord_email = building['contact']['email']
                title = building['website']['title']
                if title == "":
                    title = building['name']
                yield Request(url=external_link,
                              callback=self.populate_item,
                              meta={
                                "landlord_email":landlord_email,
                                'landlord_phone' : building['contact']['phone'],
                                'landlord_name' : building['contact']['name'],
                                'description' : building['details']['overview'],
                                'latitude' : building['geocode']['latitude'],
                                'longitude' :  building['geocode']['longitude'],
                                'title' : title,
                                'parking' : parking,
                                'pets_allowed' : pets_allowed,
                                'property_type' : property_type,
                                'external_id' : external_id,
                                  'square_meters'  : square_meters
                                })


    # 3. SCRAPING level 3
    def populate_item(self, response):
        description =remove_unicode_char((((' '.join(response.css('.main > p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        try:
            address = response.css('.address::text').extract_first()
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        except:
            latitude = response.meta['latitude']
            longitude = response.meta['longitude']
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.slickslider-gallery a::attr(href)').extract()
        amenities =remove_unicode_char((((' '.join(response.css('.amenity-holder::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

        furnished = None
        if any(word in description.lower() for word in self.keywords['furnished']) or any(word in amenities.lower() for word in self.keywords['furnished']):
            furnished = True

        parking = None
        if any(word in description.lower() for word in self.keywords['parking']) or any(word in amenities.lower() for word in self.keywords['parking']):
            parking = True

        elevator = None
        if any(word in description.lower() for word in self.keywords['elevator']) or any(word in amenities.lower() for word in self.keywords['elevator']):
            elevator = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']) or any(word in amenities.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']) or any(word in amenities.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']) or any(word in amenities.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']) or any(word in amenities.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']) or any(word in amenities.lower() for word in self.keywords['dishwasher']):
            dishwasher = True

        rentals = response.css('#slickslider-default-id-1 div')
        suite_info_container = response.css('.suite-info-container ul')
        counter = 1
        for n,rental in enumerate(rentals):
            rent = rental.css('.suite-rate::text').extract_first()
            if rent:
                rent = rent.split('-')
                rent = rent[-1]
                rent = int(extract_number_only(extract_number_only(rent)))

                room_count = None
                bathroom_count = None
                square_meters = None
                available_date = None
                for info in suite_info_container[n].css('li'):
                    header = info.css('.label::text').extract_first()
                    header = header.lower()
                    if 'bedroom' in header:
                        room_count = info.css('.info').extract_first()
                        if room_count:
                            room_count = int(extract_number_only(extract_number_only(room_count)))
                            if room_count == 0:
                                room_count = 1
                    if 'bathroom' in header:
                        bathroom_count = info.css('.info').extract_first()
                        if bathroom_count:
                            bathroom_count = int(extract_number_only(extract_number_only(bathroom_count)))

                    if 'square feet' in header:
                        square_meters = info.css('.info').extract_first()
                        if square_meters:
                            square_meters = int(extract_number_only(extract_number_only(square_meters)))

                landlord_name = response.meta['landlord_name']
                if landlord_name is None:
                    landlord_name = 'greystone residential'
                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_link", response.url+"#"+str(counter)) # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", str(response.meta['external_id'])) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", response.meta['title']) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", str(latitude)) # String
                item_loader.add_value("longitude", str(longitude)) # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", response.meta['property_type']) # String
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                item_loader.add_value("pets_allowed", response.meta['pets_allowed']) # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
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
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                # item_loader.add_value("deposit", deposit) # Int
                item_loader.add_value("currency", "CAD") # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", response.meta['landlord_phone']) # String
                item_loader.add_value("landlord_email", response.meta['landlord_email']) # String

                counter +=1
                self.position += 1
                yield item_loader.load_item()
