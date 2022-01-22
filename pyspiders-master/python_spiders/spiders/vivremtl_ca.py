# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only
from ..loaders import ListingLoader


class VivremtlCaSpider(scrapy.Spider):
    name = 'vivremtl_ca'
    allowed_domains = ['vivremtl.ca']
    start_urls = ['https://www.vivremtl.ca/']  # https not http
    country = 'canada'
    locale = 'en'
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
        images = response.css('.gallery-images a::attr(href)').extract()
        amenities = " ".join(response.css('.amenity::text').extract())
        amenities = amenities.lower()
        description = remove_unicode_char((((' '.join(response.css('#about p::text').extract()).replace('\n','')).replace('\t', '')).replace('\r', '')))

        yield Request(url='https://www.vivremtl.ca/fpn/default?useParentClient=&propertyId=179233',
                      callback=self.populate_item,
                      meta= {'images' : images,
                             'amenities' : amenities,
                             'description' : description
                             },
                      method="GET",
                      dont_filter = True)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        rentals = response.css('.unit-list li')
        rentals_len = int(ceil(len(rentals)/2)) +1
        rentalsA = response.css('.unit-list a')
        rentalsP = response.css('p.bedroom')
        jsons = response.css('#suite_details_container div::attr(data-floorplans)').extract()
        jsons_dict = {}
        amenities = response.meta['amenities']
        for Json in jsons:
            sndJson = re.findall('{.+},({.+})',Json)
            if sndJson:
                parsed_response = json.loads(sndJson[0])
                if parsed_response is not None:
                    parsed_response = parsed_response
                    key = parsed_response['suite_type_id']
                    jsons_dict[key] = parsed_response
        for n,rental in enumerate(rentals[:rentals_len]):
            available = rental.css('.available::attr(class)').extract_first()
            if available== 'unit-item available':
                item_loader = ListingLoader(response=response)

                external_id = rentalsA[n].css('::attr(data-unit-id)').extract_first()
                title = rental.css('.unit-number::text').extract_first()
                floor = rental.css('.unit-number::text').extract_first()
                floor = extract_number_only(floor.replace('suite ',''))
                print(floor)
                floor = str(floor[0])
                print(floor)
                address = '439 Avenue Thérèse Lavoie-Roux, Montreal, Quebec'
                # #try to get it the rest of the address attribute from geocode
                latitude = '45.52673'
                longitude = '-73.61647'
                city = 'Montreal'
                zipcode = 'H2V0B1'
                square_meters = int(rental.css('::attr(data-sqft)').extract_first())
                property_type = 'apartment'
                if rentalsP[n].css('::text').extract_first() == 'Studio':
                    property_type = 'studio'
                room_count = int(rental.css('::attr(data-beds)').extract_first())
                if room_count == 0:
                    room_count = 1
                bathroom_count = int(rental.css('::attr(data-baths)').extract_first())

                available_date =rental.css('::attr(data-available)').extract_first()
                if available_date == "":
                    available_date = None

                images = response.meta['images']
                floor_plan_images = None
                if external_id in jsons_dict.keys():
                    floorimgdict = jsons_dict[external_id]
                    if 'image' in floorimgdict.keys():
                        floor_plan_images = 'https://s3.amazonaws.com/lws_lift/vivre/images/floorplans/'+floorimgdict['image']
                rent = int(rental.css('::attr(data-price)').extract_first())
                if rent != 0:
                    parking = None
                    if 'garage' in amenities:
                        parking = True


                    terrace = None
                    if 'terrace' in amenities:
                        terrace = True

                    swimming_pool = None
                    if 'pool' in amenities:
                        swimming_pool = True

                    washing_machine = None
                    if 'laundry' in amenities:
                        washing_machine = True

                    elevator = None
                    if 'elevator' in amenities:
                        elevator = True

                    dishwasher = None
                    if 'dishwasher' in amenities:
                        dishwasher = True

                    balcony = None
                    if 'balcon' in amenities:
                        balcony = True



                    # # MetaData
                    item_loader.add_value("external_link", 'https://www.vivremtl.ca/#'+str(self.position))  # String
                    item_loader.add_value("external_source", self.external_source)  # String

                    item_loader.add_value("external_id", external_id) # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", title) # String
                    item_loader.add_value("description", response.meta['description']) # String

                    # # Property Details
                    item_loader.add_value("city", city) # String
                    item_loader.add_value("zipcode", zipcode) # String
                    item_loader.add_value("address", address) # String
                    item_loader.add_value("latitude", latitude) # String
                    item_loader.add_value("longitude", longitude) # String
                    item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", property_type) # String
                    item_loader.add_value("square_meters", square_meters) # Int
                    item_loader.add_value("room_count", room_count) # Int
                    item_loader.add_value("bathroom_count", bathroom_count) # Int

                    item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                    # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                    # item_loader.add_value("furnished", furnished) # Boolean
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
                    # item_loader.add_value("deposit", deposit) # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    # item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "CAD") # String

                    # item_loader.add_value("water_cost", water_cost) # Int
                    # item_loader.add_value("heating_cost", heating_cost) # Int

                    # item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", 'Vivre Outremont') # String
                    item_loader.add_value("landlord_phone", '(514) 443-2133') # String
                    # item_loader.add_value("landlord_email", landlord_email) # String

                    self.position += 1
                    yield item_loader.load_item()
