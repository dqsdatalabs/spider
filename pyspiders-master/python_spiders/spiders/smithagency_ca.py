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


class Smithagency_Spider(scrapy.Spider):

    name = 'smithagency'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en' 
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1
    anchor = 1

    stopParsing = False

    def start_requests(self):
        start_url = 'https://smithagency.ca/search'
        yield Request(
            start_url, callback=self.parse)

    def parse(self, response):

        pages = response.css(
            '#jomestate-grid div.mb-3.clearfix .pagination a::attr(href)').getall()
        pages = ['https://smithagency.ca'+x for x in pages]
        pages.insert(0, 'https://smithagency.ca/search')
        del pages[-1]
        del pages[-1]
        for page in pages:
            yield Request(
                page, callback=self.parseApartment, dont_filter=True)

    def parseApartment(self, response):

        apartments = response.css('.card')

        for apartment in apartments:

            property_type = apartment.css('.card .badge-cd.mr-2::text').get()
            try:
                rent = int(re.search(
                    r'\d+', "".join(apartment.css('.price .mb-1::text').getall()).replace(',', ''))[0])
            except:
                rent = 0
                continue
            title = apartment.css('.title a::text').get()
            count = apartment.css('.beds-value::text').getall()
            room_count = '1'
            bathroom_count = '1'
            if len(count) > 1:
                room_count = count[0]
                bathroom_count = count[1]

            external_id = re.search(
                r'\d+', apartment.css('.refnumber::text').get())[0]

            external_link = 'https://smithagency.ca' + \
                apartment.css('.title a::attr(href)').get()

            dataUsage = {
                'title': title,
                "external_id": external_id,
                "external_link": external_link,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "property_type": property_type,
                "rent": rent,

            }
            yield Request(external_link, meta=dataUsage, callback=self.parse_description, dont_filter=True)

    def parse_description(self, response):

        title = response.meta['title']
        external_id = response.meta["external_id"]
        room_count = response.meta["room_count"]
        bathroom_count = response.meta['bathroom_count']
        property_type = response.meta['property_type']
        rent = response.meta['rent']
        available_date = ''
        if response.css(".availability p:nth-child(2):contains('Available on')"):
            available_date = response.css(
                ".availability p:nth-child(2):contains('Available on')::text").get()

        description = remove_white_spaces(
            "".join(response.css('#description *::text').getall()))

        location = response.css('script').getall()
        rex = re.findall(r'-?\d+\.\d+', "".join(location))
        latitude = rex[0]
        longitude = rex[1]

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        washing_machine = True if response.css(
            ".d-block.amenities:contains('Laundry')") else False
        pets_allowed = True if response.css(
            ".d-block.amenities:contains('Pet')") else False

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value(
                "external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

            item_loader.add_value("pets_allowed", pets_allowed)

            #item_loader.add_value("square_meters", int(int(square_meters)*10.764))

            item_loader.add_value('available_date', available_date)
            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value("room_count", room_count)

            #item_loader.add_value("images", images)
            #item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "CAD")

            item_loader.add_value(
                "washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'Smith Agency')
            item_loader.add_value("landlord_phone", '(204) 287-2872')
            item_loader.add_value(
                "landlord_email", 'smithagency@smithagency.ca')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
