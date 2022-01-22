# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class RentnorthviewComSpider(scrapy.Spider):
    name = 'rentnorthview_com'
    allowed_domains = ['rentnorthview.com','api.theliftsystem.com']
    start_urls = ['https://www.rentnorthview.com/scripts/main.js?d=1638391886']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def parse(self, response):
        token = re.findall('arch.config.authToken="(\w+)"',response.text)
        yield scrapy.Request('https://www.rentnorthview.com/apartments/cities', callback=self.cityparse, method="GET", meta={'token' : token[0]})

    # 2. SCRAPING level 2
    def cityparse(self, response, **kwargs):
        rentals = response.css('#content .cities a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://www.rentnorthview.com/'+rental,
                          callback=self.buildingparse,
                          meta={'token' : response.meta['token']})

    def buildingparse(self, response):
        data_client_id = response.css('.search-data::attr(data-client-id)').extract_first()
        data_client_id = data_client_id.replace(',','%2C')
        data_city_id = response.css('.search-data::attr(data-city-id)').extract_first()
        api_link = f'https://api.theliftsystem.com/v2/search?client_id={data_client_id}&auth_token={response.meta["token"]}&city_id={data_city_id}'
        yield Request(url=api_link,callback=self.apiparse)

    def apiparse(self, response):
        parsed_response = json.loads(response.body)
        for building in parsed_response:
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
            landlord_email = landlord_email.split(',')
            landlord_email = landlord_email[0]
            title = building['website']['title']
            if title is None:
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
        rentaltitle = remove_white_spaces(response.css('.building-name::text').extract_first())
        description =remove_unicode_char((((' '.join(response.css('#building-description p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.slides a::attr(href)').extract()

        amenities =remove_unicode_char((((' '.join(response.css('.amenity::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        promo =remove_unicode_char((((' '.join(response.css('.promo-link::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        deposit = re.findall('(\d+) Security Deposits!',promo)
        if deposit:
            deposit = int(deposit[0])
        else:
            deposit = None


        furnished = None
        if 'furnish' in description.lower():
            furnished = True

        elevator = None
        if 'elevator' in description.lower() or 'elevator' in amenities.lower():
            elevator = True

        balcony = None
        if 'balcon' in description.lower() or 'balcon' in amenities.lower():
            balcony = True

        terrace = None
        if 'terrace' in description.lower() or 'terrace' in amenities.lower():
            terrace = True

        swimming_pool = None
        if 'pool' in description.lower() or 'pool' in amenities.lower():
            swimming_pool = True

        washing_machine = None
        if 'laundry' in description.lower() or 'laundry' in amenities.lower():
            washing_machine = True

        dishwasher = None
        if 'dishwasher' in description.lower() or 'dishwasher' in amenities.lower():
            dishwasher = True
        rentals = response.css('.no-description')
        counter = 1
        for rental in rentals:
            if rental.css('.suite-availability> .available'):
                title = rental.css('.th-type-name::text').extract_first()
                bed_bath = re.findall('(\d) Bed',title)
                try:
                    room_count = bed_bath[0][0]
                except:
                    room_count = 1
                bathroom_count = (extract_number_only(rental.css('.suite-bath .value::text').extract_first()))
                rent = int(extract_number_only(rental.css('.hidden-not-phone span::text').extract_first()))
                if rent !=0:
                    # # MetaData
                    item_loader = ListingLoader(response=response)
                    item_loader.add_value("external_link", response.url+"#"+str(counter)) # String
                    item_loader.add_value("external_source", self.external_source)  # String

                    item_loader.add_value("external_id", str(response.meta['external_id'])) # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", rentaltitle) # String
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

                    # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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
                    item_loader.add_value("deposit", deposit) # Int
                    item_loader.add_value("currency", "CAD") # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", response.meta['landlord_name']) # String
                    item_loader.add_value("landlord_phone", response.meta['landlord_phone']) # String
                    item_loader.add_value("landlord_email", response.meta['landlord_email']) # String

                    counter +=1
                    self.position += 1
                    yield item_loader.load_item()
