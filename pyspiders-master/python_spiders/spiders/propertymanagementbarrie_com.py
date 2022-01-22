# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates
from ..loaders import ListingLoader


class PropertymanagementbarrieComSpider(scrapy.Spider):
    name = 'propertymanagementbarrie_com'
    allowed_domains = ['propertymanagementbarrie.com','api.theliftsystem.com']
    start_urls = ['https://www.propertymanagementbarrie.com/']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.parent-page:nth-child(1) .child-link::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.propertymanagementbarrie.com'+rental,
                          callback=self.buildingparse,
                          )

    def buildingparse(self, response):
        data_city_id = response.css('.search-data::attr(data-city-id)').extract_first()
        api_link = f'https://api.theliftsystem.com/v2/search?client_id=209&auth_token=sswpREkUtyeYjeoahA2i&city_id={data_city_id}&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=500&max_rate=4000&only_available_suites=true&region=&keyword=false&property_types=&city_ids=&ownership_types=&exclude_ownership_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=30&offset=0&count=false'
        yield Request(url=api_link,callback=self.apiparse)

    def apiparse(self, response):
        parsed_response = json.loads(response.body)
        for building in parsed_response:
            if building['parking']['additional'] == "" and building['parking']['indoor'] == "" and building['parking']['outdoor'] == "":
                parking = False
            else:
                parking = True
            external_link = building['permalink']
            external_link = (external_link.split('/'))[-1]
            external_link = 'https://www.propertymanagementbarrie.com/apartments-for-rent/' + external_link
            external_id = building['id']
            property_type = building['property_type']
            if 'home' in property_type or 'house' in property_type:
                property_type = 'house'
            else:
                property_type = 'apartment'
            pets_allowed = building['pet_friendly']
            if pets_allowed == 'n/a':
                pets_allowed = None
            square_meters = int(ceil(float(building['statistics']['suites']['square_feet']['max'])))
            if square_meters == 0:
                square_meters = None
            landlord_email = building['client']['email']
            title = building['website']['title']
            if title == '':
                title = building['name']
            yield Request(url=external_link,
                          callback=self.populate_item,
                          meta={
                            "landlord_email":landlord_email,
                            'landlord_phone' : building['client']['phone'],
                            'landlord_name' : building['client']['name'],
                            'description' : building['details']['overview'],
                            'latitude' : building['geocode']['latitude'],
                            'longitude' :  building['geocode']['longitude'],
                              'address' : building['address']['address'],
                            'title' : title,
                            'parking' : parking,
                            'pets_allowed' : pets_allowed,
                            'property_type' : property_type,
                            'external_id' : external_id,
                              'square_meters'  : square_meters
                            })


    # 3. SCRAPING level 3
    def populate_item(self, response):
        description =remove_unicode_char((((' '.join(response.css('section.bottom-details.extra-padded p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        address = response.meta['address']
        images = response.css('.slides img::attr(src)').extract()

        amenities =remove_unicode_char((((' '.join(response.css('.amenities li::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))


        furnished = None
        if 'furnish' in description.lower() or 'furnish' in amenities.lower():
            furnished = True

        elevator = None
        if 'elevator' in description.lower() or 'elevator' in amenities.lower():
            elevator = True

        balcony = None
        if 'balcon' in description.lower() or 'balcon' in amenities.lower():
            balcony = True

        terrace = None
        if 'yard' in description.lower() or 'yard' in amenities.lower():
            terrace = True

        swimming_pool = None
        if 'pool' in description.lower() or 'pool' in amenities.lower():
            swimming_pool = True

        washing_machine = None
        if 'laundry' in description.lower() or 'laundry' in amenities.lower() or 'washer' in description.lower() or 'washer' in amenities.lower():
            washing_machine = True

        dishwasher = None
        if 'dishwasher' in description.lower() or 'dishwasher' in amenities.lower():
            dishwasher = True
        rentals = response.css('.suite')
        counter = 1
        for rental in rentals:
            if rental.css('.available'):
                title = rental.css('.suite-type::text').extract_first()
                bed_bath = re.findall('(\d)?\+?(\d) Bed',title)
                if len(bed_bath) == 0:
                    room_count = re.findall('(\d) bed',description.lower())
                    if len(room_count) >0:
                        room_count = int(room_count[0])
                    else:
                        room_count = 1
                elif bed_bath[0][0] != '':
                    bed_bath = [int(i) for i in bed_bath[0]]
                    room_count = sum(bed_bath)
                else:
                    room_count = bed_bath[0][1]
                bathroom_count = int(ceil(float(rental.css('.suite-bath .value::text').extract_first())))
                rent = int(extract_number_only(rental.css('.suite-rate .value::text').extract_first()))
                available_date=rental.css('.available ::text').extract()
                available_date = ' '.join(available_date)
                available_date = re.findall('(\w+) (\d+), (\d+)',available_date)
                if available_date:
                    available_date = '20'+available_date[0][-1]+'-'+available_date[0][0]+'-'+available_date[0][1]
                else:
                    available_date = 'available now'
                extra_photos= rental.css('.suite-photos a::attr(href)').extract()
                images = images + extra_photos
                if rent !=0:
                    # # MetaData
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
                    item_loader.add_value("square_meters", response.meta['square_meters']) # Int
                    item_loader.add_value("room_count", room_count) # Int
                    item_loader.add_value("bathroom_count", bathroom_count) # Int

                    item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                    item_loader.add_value("pets_allowed", response.meta['pets_allowed']) # Boolean
                    item_loader.add_value("furnished", furnished) # Boolean
                    item_loader.add_value("parking", response.meta['parking']) # Boolean
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
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    # item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "CAD") # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", response.meta['landlord_name']) # String
                    item_loader.add_value("landlord_phone", response.meta['landlord_phone']) # String
                    item_loader.add_value("landlord_email", response.meta['landlord_email']) # String

                    counter +=1
                    self.position += 1
                    yield item_loader.load_item()
