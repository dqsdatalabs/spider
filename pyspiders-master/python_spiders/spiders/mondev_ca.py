# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re

import requests
import scrapy
from scrapy import Request

from ..helper import sq_feet_to_meters, remove_unicode_char, extract_number_only
from ..loaders import ListingLoader


class MondevCaSpider(scrapy.Spider):
    name = "mondev_ca"
    allowed_domains = ['mondev.ca']
    start_urls = ['https://www.mondev.ca/apartments-for-rent-montreal',
                  ]
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
        parsed_response = response.xpath("//discover-component").get()
        regex = re.findall(":all-projects='(\[{.+}])", parsed_response)
        parsed_response = json.loads(regex[0])
        for item in parsed_response:
            if (item['comingSoon'] == '') and (item['external_url'] == ''):
                external_link = item['url']
                external_source = self.external_source
                longitude = item['longitude']
                title = item['title']
                latitude = item['latitude']
                images_json = item['gallery']
                images = []
                for i in images_json:
                    images.append(i['src'])
                yield Request(url=external_link,
                              callback=self.populate_item,
                              meta={
                                 'external_link' : external_link,
                                  'external_source' : external_source,
                                  'longitude': longitude,
                                  'title' : title,
                                  'latitude' : latitude,
                                  'images' : images,
                                  'external_images_count' : len(images)
                              })


    # 3. SCRAPING level 3
    def populate_item(self, response):
        counter = 0
        description = remove_unicode_char((((' '.join(response.css('.project-text ::text').extract()).replace('\n','')).replace('\t', '')).replace('\r', '')))
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={response.meta['longitude']},{response.meta['latitude']}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        rents_html = response.css('.row span::text').extract()
        rents_html = [i.replace('\xa0',' ') for i in rents_html]
        rents_html = [re.findall('(Studio|\d-bedroom) starting at .(\d+,\d+|\d+)', i) for i in rents_html]
        rents_html = [i for i in rents_html if i != []]
        rents_dict = {}
        if rents_html != []:
            for j in rents_html:
                header = (j[0][0])
                if header == 'Studio':
                    rents_dict['studio'] = extract_number_only(extract_number_only((j[0][1])))
                elif header == '1-bedroom':
                    rents_dict['1'] = extract_number_only(extract_number_only((j[0][1])))
                elif header == '2-bedroom':
                    rents_dict['2'] = extract_number_only(extract_number_only((j[0][1])))
                elif header == '3-bedroom':
                    rents_dict['3'] = extract_number_only(extract_number_only((j[0][1])))
                elif header == '4-bedroom':
                    rents_dict['4'] = extract_number_only(extract_number_only((j[0][1])))
                elif header == '5-bedroom':
                    rents_dict['5'] = extract_number_only(extract_number_only((j[0][1])))

        amenities = (''.join(response.css('.project-amenities > ul > li > div::text').extract())).lower()

        furnished = None
        if 'furnish' in description:
            furnished = True
        else:
            furnished = False

        parking = None
        if 'parking' in amenities:
            parking = True
        else:
            parking = False

        elevator = None
        if 'elevator' in amenities:
            elevator = True
        else:
            elevator = False

        balcony = None
        if 'balcony' in amenities:
            balcony = True
        else:
            balcony = False

        terrace = None
        if 'terrace' in amenities:
            terrace = True
        else:
            terrace = False

        swimming_pool = None
        if 'pool' in description:
            swimming_pool = True
        else:
            swimming_pool = False

        washing_machine = None
        if 'washer' in amenities:
            washing_machine = True
        else:
            washing_machine = False

        dishwasher = None
        if 'dishwasher' in amenities:
            dishwasher = True
        else:
            dishwasher = False
        landlord_name = response.css('.person::text').extract_first()
        if landlord_name is None:
            landlord_name = 'mondev'
        landlord_email = response.css('.email a::text').extract_first()
        landlord_phone = response.css('.phone a::text').extract_first()

        parsed_response = response.xpath("//units-component").get()
        app_regex = re.findall(":units='(\[{.+}])", parsed_response)
        parsed_response = json.loads(app_regex[0])
        available_count = 0
        for unit in parsed_response:
            if unit['isAvailable'] == '1':
                available_count += 1
                external_link = response.meta['external_link'] +'#'+str(counter)
                floor_plan_images = unit['planImages']
                floor = unit['floor']
                external_id = unit['id']
                square_meters = sq_feet_to_meters(unit['area'])
                room_count = unit['bedrooms']
                if 'Studio' in room_count:
                    room_count = 1
                    property_type = 'studio'
                elif 'Penthouse' in room_count:
                    room_count = 1
                    property_type = 'house'
                else:
                    property_type = 'apartment'
                bathroom_count = unit['bathrooms']
                available_date = (unit['dateAvailableFormatted']).replace('/', '-')
                if property_type in rents_dict.keys():
                    rent = rents_dict[property_type]
                elif str(room_count) in rents_dict.keys():
                    rent = rents_dict[str(room_count)]
                else:
                    rent = list(rents_dict.keys())[0]
                    rent = rents_dict[rent]
                if rent != 0:
                    item_loader = ListingLoader(response=response)

                    # # MetaData
                    item_loader.add_value("external_link", external_link) # String
                    item_loader.add_value("external_source", response.meta['external_source']) # String

                    item_loader.add_value("external_id", external_id) # String
                    item_loader.add_value("position", self.position) # Int
                    item_loader.add_value("title", response.meta['title']) # String
                    item_loader.add_value("description", description) # String

                    # # Property Details
                    item_loader.add_value("city", city) # String
                    item_loader.add_value("zipcode", zipcode) # String
                    item_loader.add_value("address", address) # String
                    item_loader.add_value("latitude", str(response.meta['latitude'])) # String
                    item_loader.add_value("longitude", str(response.meta['longitude'])) # String
                    item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", property_type) # String
                    item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                    item_loader.add_value("room_count", int(room_count)) # Int
                    item_loader.add_value("bathroom_count", int(bathroom_count)) # Int

                    item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                    # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                    item_loader.add_value("furnished", furnished) # Boolean
                    item_loader.add_value("parking", parking) # Boolean
                    item_loader.add_value("elevator", elevator) # Boolean
                    item_loader.add_value("balcony", balcony) # Boolean
                    item_loader.add_value("terrace", terrace) # Boolean
                    item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                    item_loader.add_value("washing_machine", washing_machine) # Boolean
                    item_loader.add_value("dishwasher", dishwasher) # Boolean

                    # # Images
                    item_loader.add_value("images", response.meta['images']) # Array
                    item_loader.add_value("external_images_count", response.meta['external_images_count']) # Int
                    item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                    # # Monetary Status
                    item_loader.add_value("rent", rent) # Int
                    # item_loader.add_value("deposit", deposit) # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    # item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "CAD") # String

                    #item_loader.add_value("water_cost", water_cost) # Int
                    #item_loader.add_value("heating_cost", heating_cost) # Int

                    #item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", landlord_name) # String
                    item_loader.add_value("landlord_phone", landlord_phone) # String
                    item_loader.add_value("landlord_email", landlord_email) # String

                    self.position += 1
                    counter +=1
                    yield item_loader.load_item()
