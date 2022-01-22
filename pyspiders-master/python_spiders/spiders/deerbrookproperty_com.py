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


class Deerbrookproperty_Spider(scrapy.Spider):

    name = 'deerbrookproperty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['deerbrookproperty.com']

    position = 1

    stopParsing = False

    def start_requests(self):
        i = 0
        start_url = f'https://www.deerbrookproperty.com/residential-properties/page-{int(i)}'
        yield Request(
            start_url.format(i), callback=self.parseDetails)
        while not self.stopParsing:
            i += 24
            start_url = f'https://www.deerbrookproperty.com/residential-properties/page-{int(i)}'
            if i > 768:
                break
            yield Request(
                start_url.format(i), callback=self.parseDetails)

    def parseDetails(self, response):
        apartments = response.css('.listing_img a::attr(href)').getall()
        if len(apartments) > 0:
            for apartment in apartments:
                yield Request(
                    apartment, callback=self.parseApartment)
        else:
            self.stopParsing = True

    def parseApartment(self, response):

        title = response.css('.title h1::text').get()
        info = response.css('.py-1.flex-space-between *::text').getall()
        rent = 0
        city = ''
        room_count = 1
        bathroom_count = 1
        address = ''
        external_id = ''
        parking = False
        property_type = ''
        for i, w in enumerate(info):
            if 'eas' in w and 'For' not in w:
                rent = int(
                    (info[i+1].replace('.00', '').replace('$', '').replace(',', '')))
            if 'City' in w:
                city = info[i+1]
            if 'Bed / Bath' in w:
                room_count = info[i+1].split(' / ')[0]
                bathroom_count = info[i+1].split(' / ')[1][0]
            if 'Address' in w:
                address = info[i+1]
            if 'Listing ID' in w:
                external_id = info[i+1]
            if 'Garage' in w:
                parking = True
            if 'Type' in w:
                property_type = info[i+1]

        if rent == 0:
            return
        address += ', '+city
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        zipcode = ''
        try:
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']

            longitude = str(longitude)
            latitude = str(latitude)

        except:
            longitude = ""
            latitude = ""

        description = remove_white_spaces(response.css(
            '.container-fluid .fs-25.text-gray.text-center::text').get())

        washing_machine = True if response.css(
            ".listing_ext_features *:contains('Washer')") else False
        dishwasher = True if response.css(
            ".listing_ext_features *:contains('Dishwasher')") else False

        external_link = response.url
        images = response.css('.expand-full-screen a::attr(href)').getall()

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
            item_loader.add_value('dishwasher', dishwasher)
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
            item_loader.add_value("landlord_name", 'WINDSOR OFFICE')
            item_loader.add_value("landlord_phone", '519.972.1000')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
