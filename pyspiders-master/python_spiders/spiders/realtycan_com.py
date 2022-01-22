import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
import math
from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json


class RealtycanSpider(scrapy.Spider):

    name = 'realtycan'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['realtycan.com']
    start_urls = ["https://api.theliftsystem.com/v2/search?locale=en&client_id=687&auth_token=sswpREkUtyeYjeoahA2i&city_id=202&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=409900&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=845%2C2641%2C202&pet_friendly=&offset=0&count=false"]
    position = 1

    def parse(self, response):

        apartments = json.loads(str(response.text))

        for apartment in apartments:
            try:
                external_id = str(apartment['custom_fields']['property_id'])
            except:
                external_id = "0"

            external_link = str(apartment['permalink']).replace(
                'for-sale', "properties")
            title = apartment['name']
            property_type = 'apartment' if 'apartment' in apartment['property_type'] else 'house'
            city = apartment['address']['city']
            zipcode = apartment['address']['postal_code']
            address = apartment['address']['address'] + ', ' + \
                city + ', ' + apartment['address']['province_code']

            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']

            landlord_phone = apartment['contact']['phone']
            #landlord_email = apartment['contact']['email']
            landlord_name = apartment['contact']['name']

            room_count = apartment['statistics']['suites']['bedrooms']['max']
            bathroom_count = apartment['statistics']['suites']['bathrooms']['max']
            rent = int(apartment['statistics']['suites']['rates']['max'])
            try:
                square_meters = sq_feet_to_meters(
                    apartment['statistics']['suites']['square_feet']['average'])
            except:
                square_meters = 0
            description = remove_white_spaces(apartment['details']['overview']).replace(
                '<br>', "").replace('<p>', "").replace('</p>', "")
            
            rex = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', description)
            if rex:
                landlord_email = rex.group(0)
            else:
                landlord_email = ''

            available_date = apartment['min_availability_date']

            currency = 'CAD'

            dataUsage = {
                "property_type": property_type,
                "title": title,
                "external_id": external_id,
                "external_link": external_link,
                "city": city,
                "address": address,
                "zipcode": zipcode,
                "longitude": longitude,
                "latitude": latitude,
                'available_date': available_date,
                "description": description,
                "square_meters": square_meters,
                'landlord_phone': landlord_phone,
                'landlord_email': landlord_email,
                'landlord_name': landlord_name,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
            }
            if rent < 6000 and rent > 0:
                yield Request(external_link,
                              callback=self.parseApartment,
                              dont_filter=True, meta=dataUsage)

    def parseApartment(self, response):

        images = response.css(".gallery-image *::attr(href)").getall()
        rex = re.search(r'Security Deposit: .(\d+)',
                        response.meta['description'])

        if rex:
            deposit = rex.groups()[0]
        else:
            deposit = 0
        details = response.css(".amenity-holder")
        if details.css("*:contains('Balcony')"):
            balcony = True
        else:
            balcony = False

        if details.css("*:contains('Dishwasher')"):
            dishwasher = True
        else:
            dishwasher = False

        if details.css("*:contains('Laundry')"):
            washing_machine = True
        else:
            washing_machine = False

        if details.css("*:contains('Parking')") or details.css("*:contains('Garage')"):
            parking = True
        else:
            parking = False

        if details.css("*:contains('Elevator')"):
            elevator = True
        else:
            elevator = False

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", response.meta['title'])
        item_loader.add_value("description", response.meta['description'])
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("zipcode", response.meta['zipcode'])
        item_loader.add_value("address", response.meta['address'])
        item_loader.add_value("latitude", response.meta['latitude'])
        item_loader.add_value("longitude", response.meta['longitude'])
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", int(int(response.meta['square_meters'])*10.764))
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value(
            "bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("landlord_name", response.meta['landlord_name'])
        item_loader.add_value(
            "landlord_email", response.meta['landlord_email'])
        item_loader.add_value(
            "landlord_phone", response.meta['landlord_phone'])
        item_loader.add_value("position", self.position)
        self.position += 1
        yield item_loader.load_item()
