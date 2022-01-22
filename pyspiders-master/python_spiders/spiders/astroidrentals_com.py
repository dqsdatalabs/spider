from operator import le
from types import MethodType
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json
import math


class astroidrentals_Spider(scrapy.Spider):

    name = 'astroidrentals'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    
    def start_requests(self):
        start_url = ['https://api.theliftsystem.com/v2/search?only_available_suites=true&client_id=128&auth_token=sswpREkUtyeYjeoahA2i&city_id=3377',
        'https://api.theliftsystem.com/v2/search?only_available_suites=true&client_id=128&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133',
        'https://api.theliftsystem.com/v2/search?only_available_suites=true&client_id=128&auth_token=sswpREkUtyeYjeoahA2i&city_id=2587',
        'https://api.theliftsystem.com/v2/search?only_available_suites=true&client_id=128&auth_token=sswpREkUtyeYjeoahA2i&city_id=2275',
        'https://api.theliftsystem.com/v2/search?only_available_suites=true&client_id=128&auth_token=sswpREkUtyeYjeoahA2i']
        for url in start_url:
            yield Request(
                url, callback=self.parse)

    def parse(self, response):

        apartments = json.loads(response.text)

        for apartment in apartments:
            title = apartment['address']['address']+" "+apartment['address']['city']+','+apartment['address']['province_code']
            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']
            landlord_phone = apartment['contact']['phone']
            if landlord_phone=='':
                landlord_phone='204-338-4671'
            landlord_name =  apartment['contact']['name']
            if landlord_name=='':
                landlord_name='astroidrentals'
            landlord_email = apartment['contact']['email']
            if landlord_email=='':
                landlord_email='info@astroidinfo.com'

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
            property_type = 'apartment' if 'apartment' in apartment['property_type'] else 'house'
            external_link = apartment['permalink']


            yield Request(
                external_link, meta={
                    'title': title,
                    "landlord_phone": landlord_phone,
                    "zipcode": zipcode,
                    "city": city,
                    "address": address,
                    "latitude": latitude,
                    "longitude": longitude,
                    "landlord_name": landlord_name,
                    "landlord_email": landlord_email,
                    'external_link':external_link,
                    "property_type": property_type},
                callback=self.parseApartment)

    def parseApartment(self, response):
        title = response.meta['title']
        landlord_phone = response.meta['landlord_phone']
        zipcode = response.meta['zipcode']
        city = response.meta['city']
        address = response.meta["address"]
        latitude = response.meta["latitude"]
        longitude = response.meta['longitude']
        property_type = response.meta['property_type']
        landlord_name = response.meta['landlord_name']
        landlord_email = response.meta['landlord_email']
     

        description = remove_white_spaces(
            "".join(response.css('.cms-content p::text').getall()))

        images = response.css('.gallery-image .cover::attr(style)').getall()
        images = [re.search(r'\'.+\'',x)[0].replace('\'','') for x in images]

        #external_id = response.css(".meta-inner-wrapper:contains('Property ID') .meta-item-value::text").get()

        parking = True if response.css(".amenities-container *:contains('parking')") else False
        washing_machine = True if response.css(".amenities-container *:contains('Laundry')") else False
        pets_allowed = True 
        elevator = True if response.css(".amenities-container *:contains('levator')") else False
        dishwasher = True if response.css(".amenities-container *:contains('Dishwasher')") else False
        i=1
        suites = response.css(".suite:contains('Currently')")
        for suite in suites:
            rent = int(re.search(r'\d+',suite.css('.value.title::text').get())[0])
            room_count = suite.css('.first-word.title::text').get()[0]
            if not room_count.isdigit():
                room_count = '1'
            external_link = response.url+'#'+str(i)
            i+=1
            

            if rent > 0:

                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_source", self.external_source)
                #item_loader.add_value("external_id", external_id)
                item_loader.add_value("title", title)
                item_loader.add_value("description", description)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)

                item_loader.add_value("address", address)
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

                #item_loader.add_value("balcony", balcony)
                item_loader.add_value("elevator", elevator)

                item_loader.add_value("pets_allowed", pets_allowed)

                #item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                #item_loader.add_value("floor_plan_images", floor_plan_images)
                item_loader.add_value("dishwasher", dishwasher)

                #item_loader.add_value('available_date', available_date)
                item_loader.add_value(
                    "property_type", property_type)
                item_loader.add_value("room_count", room_count)
                #item_loader.add_value("bathroom_count", bathroom_count)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "CAD")
                item_loader.add_value("parking", parking)
                item_loader.add_value(
                    "washing_machine", washing_machine)
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("landlord_email", landlord_email)

                item_loader.add_value("position", self.position)
                self.position += 1
                yield item_loader.load_item()
