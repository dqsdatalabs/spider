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


class Petersonrentals_Spider(scrapy.Spider):

    name = 'petersonrentals'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    #allowed_domains = ['petersonrentals.com']

    position = 1

    stopParsing = False

    def start_requests(self):
        start_url = 'https://petersonrentals.com/'
        yield Request(
            start_url, callback=self.parse)

    def parse(self, response):
        apartments = response.css('.col.property a::attr(href)').getall()
        apartments = [x for x in apartments if 'petersonrentals' in x]

        for apartment in apartments:

            yield Request(
                apartment,
                callback=self.parseApartment)

    def parseApartment(self, response):

        address = remove_white_spaces("".join(response.css(
            ".col.col20:contains('Address')::text").getall()))
        city = remove_white_spaces("".join(response.css(
            ".col.col20:contains('City')::text").getall()))
        address += ',' + city

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']

        longitude = str(longitude)
        latitude = str(latitude)

        description = remove_white_spaces(
            "".join(response.css('.large p:not(a)::text').getall()))
        images = response.css('.gallery .sub-gallery img::attr(src)').getall()

        parking = True if response.css(
            ".col.col30 *:contains('arking')") else False

        units = response.css('.unit')
        i = 1
        for unit in units:

            title = response.css('.entry-title::text').get() + \
                ' ' + unit.css('h2::text').get()
            square_meters = sq_feet_to_meters(
                re.search(r'\d+', "".join(unit.css("*:contains('SQFT')::text").getall()))[0])
            dishwasher = True if unit.css(
                "*:contains('Dishwasher')") else False
            washing_machine = True if unit.css(
                "*:contains('aundry')") else False
            pets_allowed = True if unit.css(
                "*:contains('Pet')") or response.css(".col.col30 *:contains('Pet')") else False
            available_date = remove_white_spaces("".join(unit.css(
                "*:contains('Available')::text").getall()).replace('\n', '').replace('Available ', ''))
            rent = int(
                re.search(r'\d+', "".join(unit.css("*:contains('$')::text").getall()))[0])
            floor_plan_images = [unit.css('.download-link::attr(href)').get()]
            property_type = ''
            room_count=''
            if unit.css("*:contains('Bedroom')"):
                property_type = 'apartment'
                room_count = re.search(
                r'\d+', "".join(unit.css("*:contains('Bedroom')::text").getall()))[0]
            if unit.css("*:contains('Studio')"):
                property_type = 'studio'
            if property_type == '':
                property_type = 'apartment'
            
            if room_count=='':
                room_count = '1'
            bathroom_count = 1

            external_link = response.url + '#'+str(i)
            i += 1

            if rent > 0:

                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value(
                    "external_id", unit.css('h2::text').get())
                item_loader.add_value("title", title)
                item_loader.add_value("description", description)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address", address)
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

                item_loader.add_value("pets_allowed", pets_allowed)

                item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                item_loader.add_value("floor_plan_images", floor_plan_images)
                item_loader.add_value("dishwasher", dishwasher)

                item_loader.add_value('available_date', available_date)
                item_loader.add_value(
                    "property_type", property_type)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value(
                    "bathroom_count", bathroom_count)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "CAD")
                item_loader.add_value("parking", parking)
                item_loader.add_value(
                    "washing_machine", washing_machine)
                item_loader.add_value("landlord_name", 'petersonrentals')
                item_loader.add_value("landlord_phone", response.css(
                    ".inquiries .u-no-bullets *:contains('D')::text").get().replace('D ', ''))
                item_loader.add_value("landlord_email", response.css(
                    ".inquiries .u-no-bullets a::text").get())

                item_loader.add_value("position", self.position)
                self.position += 1
                yield item_loader.load_item()
