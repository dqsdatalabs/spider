# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import requests
import scrapy
from bs4 import BeautifulSoup
from scrapy import Request

from ..helper import remove_unicode_char, remove_white_spaces, extract_number_only
from ..loaders import ListingLoader
import json as JSON


class MaclarenlivingCaSpider(scrapy.Spider):
    name = 'maclarenliving_ca'
    allowed_domains = ['maclarenliving.ca']
    start_urls = ['https://www.maclarenliving.ca/']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.description_parse)

    def description_parse(self, response):
        description =remove_unicode_char((((' '.join(response.css('#neighborhood-section .section-content::text , #apt-search-section .section-content::text , .slider-content-one::text , #InnerContentDiv span::text').extract())).replace('\n','')).replace('\t','')).replace('\r',''))
        url = 'https://www.maclarenliving.ca/amenities.aspx'
        url_get = requests.get(url)
        soup = BeautifulSoup(url_get.content, 'lxml')
        col = soup.find_all('ul',class_=['amenities-list'])
        amenities=[]
        for i in col:
            for j in i.find_all('li'):
                amenities.append(j.getText())
        amenities = ' '.join(amenities)
        amenities = amenities.lower()


        url = 'https://www.maclarenliving.ca/photogallery.aspx'
        images = []
        url_get = requests.get(url)
        soup = BeautifulSoup(url_get.content, 'lxml')
        col = soup.find_all(class_=['photogallery-image'])
        for image in col:
            image_meta= image.find('img')
            images.append(image_meta['src'])

        images = list(dict.fromkeys(images))
        images = [i.replace(' ','%20') for i in images]

        script = response.css("script:contains('latitude')").get()
        latitude = re.findall('var latitude = "(-?[\d|\.]+)',script)
        longitude = re.findall('var longitude = "(-?[\d|\.]+)', script)

        yield Request(url='https://www.maclarenliving.ca/floorplans',
                      callback=self.parse,
                      meta={'description' : description,
                            'images' : images,
                            'amenities' : amenities,
                            'latitude': latitude[0],
                            'longitude' : longitude[0]})


    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        json = response.css("script:contains('var pageData')").get()
        json = remove_white_spaces(json)
        json = re.findall('({.+});',json)
        json = json[0]
        json = re.sub('\s(\w+):',r'"\1":',json)
        parsed_response = JSON.loads(json)
        description = response.meta['description']
        for rental in parsed_response['floorplans']:
            item_loader = ListingLoader(response=response)
            if 'lowPrice' in rental or 'highPrice' in rental:
                external_id = str(rental['id'])
                title = rental['name']
                square_meters = extract_number_only(extract_number_only(rental['sqft']))
                room_count = rental['beds']
                bathroom_count = rental['baths']
                if 'lowPrice' in rental and 'highPrice' in rental:
                    rent = (rental['lowPrice'] + rental['highPrice'])/2
                elif 'lowPrice' in rental:
                    rent = rental['lowPrice']
                elif 'highPrice' in rental:
                    rent = rental['highPrice']
                floor_plan_images = []
                for i in rental['images']:
                    floor_plan_images.append(i['src'])

                latitude = response.meta['latitude']
                longitude = response.meta['longitude']
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']

                property_type = 'apartment'

                pets_allowed = None
                if 'pet' in response.meta['amenities']:
                    pets_allowed = True

                parking = None
                if 'parking' in response.meta['amenities']:
                    parking = True

                elevator = None
                if 'elevator' in response.meta['amenities']:
                    elevator = True


                balcony = None
                if 'balcon' in response.meta['amenities']:
                    balcony = True

                terrace = None
                if 'terrace' in response.meta['amenities']:
                    terrace = True

                washing_machine = None
                if ' washer' in response.meta['amenities']:
                    washing_machine = True


                # # MetaData
                item_loader.add_value("external_link", response.url+'#'+str(self.position))  # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String
                item_loader.add_value("square_meters", int(square_meters)) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                # item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", response.meta['images']) # Array
                item_loader.add_value("external_images_count", len(response.meta['images'])) # Int
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
                item_loader.add_value("landlord_name", 'maclaren living') # String
                item_loader.add_value("landlord_phone", '(780) 851-4163') # String
                # item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()

