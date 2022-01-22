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


class Rinomato_Spider(scrapy.Spider):

    name = 'rinomato'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    stopParsing = False

    def start_requests(self):
        start_url = 'https://rinomato.com/Available-Listings?display=List&filter=Lease&sold_leased=0'
        yield Request(
            start_url, callback=self.parse)

    def parse(self, response):

        apartments = response.css('.property-listing-simple')

        for apartment in apartments:
            title = apartment.css('.entry-title a::text').get()
            rent = apartment.css('.price::text').get().replace(',', '')
            sq = apartment.css(".meta-item-value:contains('-')::text").get()
            square_meters = '0'
            if sq:
                num = (int(sq.split('-')[0])+int(sq.split('-')[1]))/2
                square_meters = sq_feet_to_meters(num)
            else:
                square_meters = 0

            room_cnt = apartment.css(
                ".meta-inner-wrapper:contains('Bedrooms') .meta-item-value::text").get()
            room_count = 1
            if '+' in room_cnt:
                room_count = int(room_cnt.split(
                    ' + ')[0]) + int(room_cnt.split(' + ')[1])
            else:
                room_count = int(room_cnt)
            bathroom_count = apartment.css(
                ".meta-inner-wrapper:contains('Bathroom') .meta-item-value::text").get()
            parking = True if apartment.css(
                ".meta-inner-wrapper:contains('Parking') .meta-item-value::text").get() != '0' else False
            property_type = 'apartment' if apartment.css(
                ".meta-inner-wrapper:contains('Type') .meta-item-value::text").get() == 'Apartment' else 'house'

            url = 'https://rinomato.com' + \
                apartment.css('a::attr(href)').get()
            yield Request(
                url, meta={
                    'title': title,
                    "parking": parking,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "property_type": property_type,
                    "rent": rent},
                callback=self.parseApartment, dont_filter=True)

    def parseApartment(self, response):
        title = response.meta['title']
        parking = response.meta["parking"]
        room_count = response.meta["room_count"]
        bathroom_count = response.meta['bathroom_count']
        property_type = response.meta['property_type']
        rent = response.meta['rent']
        square_meters = response.meta['square_meters']

 
        zipcode = ''
        city = ''
        longitude = ''
        latitude = ''
        address = response.css('p.address::text').get()

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
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)
        except:
            pass

        description = remove_white_spaces(
            "".join(response.css('.property-content p::text').getall()))

        images = response.css('.swipebox::attr(href)').getall()
        images = ['https://rinomato.com'+x for x in images]
        external_id = response.css(
            ".meta-inner-wrapper:contains('Property ID') .meta-item-value::text").get()

        dishwasher = True if 'Dishwasher' in description else False
        washing_machine = True if 'Washer' in description else False
        pets_allowed = False if 'No Pets' in description else True
        balcony = True if 'Balcony' in description else False
        furnished = True if 'Furnished' in description else False
        landlord_name = response.css('.agent-name a::text').get()

        if response.css(".office:contains('Office') span"):
            landlord_phone = response.css(
                ".office:contains('Office') span::text").get().replace('Office: ', '')
        else:
            landlord_phone = '416-565-3001'

        rent = int(
            re.search(r'\d+', rent)[0])

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value(
                "external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

            item_loader.add_value("balcony", balcony)
            item_loader.add_value("furnished", furnished)

            item_loader.add_value("pets_allowed", pets_allowed)

            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            #item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("dishwasher", dishwasher)

            #item_loader.add_value('available_date', available_date)
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
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", 'contact@rinomato.com')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
