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


class Metropropertymanagement_Spider(scrapy.Spider):

    name = 'metropropertymanagement'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    stopParsing = False

    def start_requests(self):
        for i in range(1, 5):
            start_url = f'https://metropropertymanagement.ca/rental-listings.php?active=1&keywords=&price=&page={i}'
            yield Request(
                start_url, callback=self.parse)

    def parse(self, response):

        apartments = response.css('.row.article')

        for apartment in apartments:
            title = apartment.css('.list-item h2::text').get()
            rent = apartment.css(
                ".list-item .price *::text").get().replace(',', '')
            rooms = apartment.css(
                ".list-item *:contains('Beds |')::text").get().split(' | ')
            room_count = '1'
            bathroom_count = '1'
            if 'Bed' in rooms[0]:
                room_count = rooms[0][0]
            if 'Bath' in rooms[1]:
                bathroom_count = rooms[1][0]

            url = 'https://metropropertymanagement.ca' + \
                apartment.css('a::attr(href)').get()
            yield Request(
                url, meta={
                    'title': title,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "rent": rent},
                callback=self.parseApartment, dont_filter=True)

    def parseApartment(self, response):
        title = response.meta['title']
        room_count = response.meta["room_count"]
        bathroom_count = response.meta['bathroom_count']
        rent = response.meta['rent']

        zipcode = ''
        city = ''
        longitude = ''
        latitude = ''
        address = response.css(
            ".table tbody tr *:contains(Address) strong::text").get()
        city = response.css(
            ".table tbody tr *:contains(City) strong::text").get()
        address += ', '+city
        Province = response.css(".table tbody tr *:contains(Province) strong::text").get()
        address += ','+Province
        landlord_phone=''
        if response.css(".table tbody tr *:contains(Phone) strong") and len(response.css(".table tbody tr *:contains(Phone) strong::text").get())>4:
            landlord_phone = 'response.css(".table tbody tr *:contains(Phone) strong::text").get()'

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
            #city = responseGeocodeData['address']['City']
            #address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)
        except:
            pass

        description = remove_white_spaces(
            "".join(response.css('#propertydesc *::text').getall())).replace('Call us for your viewing today at 763-5383! Metro Property Management','').replace('Call us today at 763-5383 for your viewing! Metro Property Management;','')

        

        dishwasher = True if response.css(
            ".table tbody tr *:contains(Dishwasher) strong") else False
        washing_machine = True if response.css(
            ".table tbody tr *:contains(Washer) strong") else False
        parking = True if response.css(
            ".table tbody tr *:contains(Parking) strong") else False
        #pets_allowed = False if 'No Pets' in description else True
        #balcony = True if 'Balcony' in description else False
        #furnished = True if 'Furnished' in description else False
        landlord_phone = response.css(
            ".table tbody tr *:contains(Phone) strong::text").get()
        landlord_email = response.css(
            ".table tbody tr *:contains(Email) strong a::text").get()
        property_type='apartment'
        try:
            property_type = 'apartment' if 'partment' in response.css(".table tbody tr *:contains(Style) strong::text").get() else 'house'
        except:
            if 'house' in description or 'House' in description:
                property_type='house'

        
        landlord_name = 'metro property management'
        available_date = response.css(
            ".table tbody tr *:contains(Date) strong::text").get()
        if response.css(".office:contains('Office') span") and len(landlord_phone)==0:
            landlord_phone = response.css(
                ".office:contains('Office') span::text").get().replace('Office: ', '')
        if len(landlord_phone)==0:
            landlord_phone = '416-565-3001'

        images = response.css('#gallery img::attr(src)').getall()

        rent = int(
            re.search(r'\d+', rent)[0])

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
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
            #item_loader.add_value("furnished", furnished)

            #item_loader.add_value("pets_allowed", pets_allowed)

            #item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            #item_loader.add_value("floor_plan_images", floor_plan_images)
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
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", landlord_email)

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
