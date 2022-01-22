from operator import le
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


class Stranoproperties_Spider(scrapy.Spider):

    name = 'stranoproperties'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        for i in range(1, 4):
            link = f'https://www.stranoproperties.ca/search-property-result/page/{i}/?location&category&bedrooms=0&bathrooms=0&min_price=0&max_price=0'
            yield Request(link, callback=self.parseDetails, dont_filter=True)

    def parseDetails(self, response):

        apartments = response.css('.noo_property')

        for apartment in apartments:
            title = remove_white_spaces(
                apartment.css('.property-title a::text').get())
            property_type = apartment.css('.property-category a::text').get()
            if 'Commercial' not in property_type:
                property_type = 'apartment' if 'Apartment' in property_type else 'house'
                external_link = apartment.css(
                    '.property-title a::attr(href)').get()
                r = apartment.css('.style-2 .property-price span::text').get()
                rent = 0
                if r:
                    rex = re.search(r'\d+', r)
                    if len(rex[0]) > 1:
                        rent = int(rex[0])
                    else:
                        rex = re.search(r'\d+\,\d+', r)
                        rent = int(rex[0].replace(',', ''))
                yield Request(external_link,
                              meta={
                                  'title': title, 'property_type': property_type, 'rent': rent},
                              callback=self.parseApartment)

    def parseApartment(self, response):

        title = response.meta['title']
        property_type = response.meta['property_type']
        rent = response.meta['rent']

        description = remove_white_spaces(
            "".join(response.css('.property-content p::text').getall()))

        if 'furnished' in description:
            furnished = True
        else:
            furnished = False

        rex = re.search(r'Available (.+) for \$',description)
        if rex:
            available_date = rex.groups()[0]
        else:
            available_date = ''
        room_count = response.css('.value-_bedrooms::text').get()[0]
        if not room_count[0].isdigit():
            room_count = 1
        bathroom_count = response.css('.value-_bathrooms::text').get()[0]
        parking = True if response.css(
            '.value-_noo_property_field_parking::text').get() else False
        address = response.css('.property-title small::text').get()

        images = response.css('a.noo-lightbox-item::attr(href)').getall()
        washing_machine = True if response.css(
            ".property-feature-content .has:contains('Laundry')") else False
        pets_allowed = True if response.css(
            ".property-feature-content .has:contains(' Pet Friendly')") else False
        dishwasher = True if 'dishwasher' in description else False
        external_images_count = len(images)

        zipcode = ''
        city = ''
        try:
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']

            longitude = str(longitude)
            latitude = str(latitude)

        except:
            longitude = ""
            latitude = ""

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)

            item_loader.add_value("furnished", furnished)
            item_loader.add_value("available_date", available_date)

            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value('washing_machine', washing_machine)
            item_loader.add_value('pets_allowed', pets_allowed)

            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value('parking', parking)
            item_loader.add_value('dishwasher', dishwasher)

            item_loader.add_value("landlord_name", 'stranoproperties')
            item_loader.add_value(
                "landlord_email", 'rentals@stranoproperties.ca')
            item_loader.add_value("landlord_phone", '(519)601 6799')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()