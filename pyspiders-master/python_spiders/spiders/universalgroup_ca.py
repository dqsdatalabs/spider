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


class Universalgroup_Spider(scrapy.Spider):

    name = 'universalgroup'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['universalgroup.ca']

    position = 1

    stopParsing = False

    def start_requests(self):
        start_url = 'https://universalgroup.ca/find-property?type=residential&price_min=500&price_max=5000&number_bedrooms=0&location=0&building=0&available_time=0'
        yield Request(
            start_url, callback=self.parse)

    def parse(self, response):
        apartments = response.css('.building.info-window-content')
        for apartment in apartments:
            url = apartment.css('h3 a::attr(href)').get()
            latitude = response.css('::attr(data-latitude)').get()
            longitude = response.css('::attr(data-longitude)').get()
            yield Request(
                'https://universalgroup.ca'+url,
                meta={
                    'latitude': latitude,
                    'longitude': longitude
                },
                callback=self.parseApartment)

    def parseApartment(self, response):

        title = response.css('.page-title::text').get()
        parking = True if response.css(
            ".field-content:contains('parking')") else False
        washing_machine = True if response.css(
            ".field-content:contains('aundry')") else False
        balcony = True if response.css(
            ".field-content:contains('balcon')") else False
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']

        zipcode = ''
        city = ''
        address = 0
        try:
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            address = responseGeocodeData['address']['Match_addr']
            city = responseGeocodeData['address']['City']

            longitude = str(longitude)
            latitude = str(latitude)

        except:
            longitude = ""
            latitude = ""

        property_type = 'apartment'

        units = response.css('.view-residential-units .views-row')
        images = response.css('#ninja-slider a::attr(href)').getall()
        description = remove_white_spaces(
            "".join(response.css('.field-content p::text').getall()))
        i = 1
        for unit in units:
            external_link = response.url + '#'+str(i)
            i += 1
            floor_plan_images = unit.css(
                '.unit-floorplan a::attr(href)').getall()
            count = unit.css(
                ".unit-fields .field-content:contains('Bedroom') *::text").get()
            room_count = 1
            bathroom_count = 1
            if count:
                if 'Two' in count:
                    room_count = 2
                if 'Three' in count:
                    room_count = 3
            bathroom_count = unit.css(
                ".unit-fields .views-field-field-number-of-bathrooms .field-content::text").get()
            external_id = unit.css(
                ".unit-fields .views-field-field-unit .field-content::text").get()
            available_date = unit.css(
                ".unit-fields .views-field-field-date-available .field-content *::text").get()
            square_meters = sq_feet_to_meters(
                unit.css(".unit-fields .views-field-field-sqft .field-content::text").get())
            rent = int(unit.css(".unit-price-links .field-content::text").get().replace(
                '.00', '').replace('$', '').replace(',', ''))

            if rent > 0:

                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("external_id", external_id)
                item_loader.add_value("title", title)
                item_loader.add_value("description", description)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address", address)
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

                item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                item_loader.add_value("floor_plan_images", floor_plan_images)
                item_loader.add_value("balcony", balcony)

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
                item_loader.add_value("landlord_name", response.css(
                    '.sidebars .views-field.views-field-title .field-content::text').get())
                item_loader.add_value("landlord_phone", response.css(
                    '.sidebars .views-field-field-phone .field-content::text').get())
                item_loader.add_value("landlord_email", response.css(
                    '.sidebars .views-field-field-email .field-content a::text').get())

                item_loader.add_value("position", self.position)
                self.position += 1
                yield item_loader.load_item()
