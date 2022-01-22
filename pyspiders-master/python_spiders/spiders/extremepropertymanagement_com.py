# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class ExtremepropertymanagementComSpider(scrapy.Spider):
    name = 'extremepropertymanagement_com'
    allowed_domains = ['extremepropertymanagement.com','api.theliftsystem.com']
    start_urls = ['https://www.extremepropertymanagement.com/apartments-for-rent']  # https not http
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
    def _parse(self, response, **kwargs):
        city_links = response.css('.cities-list a::attr(href)').extract()
        for city_link in city_links:
            yield Request(url='https://www.extremepropertymanagement.com/'+city_link,callback=self.apipost)

    def apipost(self, response):
        data_city_id = response.css('#search_data::attr(data-city-id)').extract_first()
        api_link = f'https://api.theliftsystem.com/v2/search?client_id=316&auth_token=sswpREkUtyeYjeoahA2i&city_id={int(data_city_id)}'
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
        description =remove_unicode_char((((' '.join(response.css('.cms-content p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.gallery-image a::attr(href)').extract()
        amenities =remove_unicode_char((((' '.join(response.css('.amenity::text , .suite-amenities li::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))


        rentals = response.css('.suite')
        counter = 1
        for rental in rentals:
            if rental.css('.available'):
                rent = rental.css('.rate-value::text').extract_first()
                if rent:
                    rent = rent.split('-')
                    rent = rent[-1]
                    rent = int(extract_number_only(extract_number_only(rent)))
                    ind_description = remove_unicode_char((((' '.join(rental.css('p ::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
                    if ind_description:
                        description = ind_description.replace('Leave This Blank ','')
                    room_count = rental.css('.type-name::text').extract_first()
                    room_count = re.findall('(\d) bed', room_count.lower())
                    if len(room_count) > 0:
                        room_count = int(extract_number_only(room_count[0]))
                    else:
                        room_count = re.findall('(\d)\s?\w* bed', ind_description.lower())
                        if len(room_count) > 0:
                            room_count = int(extract_number_only(room_count[0]))
                        else:
                            room_count = 0
                    available_date = 'available now'

                    bathroom_count = re.findall('(\d)\s?\w* bath', ind_description.lower())
                    if len(bathroom_count) > 0:
                        bathroom_count = int(extract_number_only(bathroom_count[0]))
                    else:
                        if 'bathroom' in ind_description:
                            bathroom_count = 1
                        else:
                            bathroom_count = None

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
                    item_loader.add_value("latitude", latitude) # String
                    item_loader.add_value("longitude", longitude) # String
                    # item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", response.meta['property_type']) # String
                    # item_loader.add_value("square_meters", square_meters) # Int
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
                    item_loader.add_value("landlord_name", response.meta['landlord_name']) # String
                    item_loader.add_value("landlord_phone", response.meta['landlord_phone']) # String
                    item_loader.add_value("landlord_email", response.meta['landlord_email']) # String

                    counter +=1
                    self.position += 1
                    yield item_loader.load_item()
