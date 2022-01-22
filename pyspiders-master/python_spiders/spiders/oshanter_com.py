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


class Oshanter_Spider(scrapy.Spider):

    name = 'oshanter'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        url = 'https://api.theliftsystem.com/v2/search?client_id=226&auth_token=sswpREkUtyeYjeoahA2i&city_id=1837&geocode=&min_bed=0&max_bed=4&min_bath=0&max_bath=10&min_rate=0&max_rate=2000&local_url_only=true&region=&keyword=false&property_types=&order=max_rate+ASC,+min_rate+ASC,+min_bed+ASC,+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=3133,1837&pet_friendly=&offset=0&count=false'
        yield Request(url, callback=self.parseDetails, dont_filter=True)


        

    def parseDetails(self, response):
        
        apartments = apartments = json.loads(response.text)

        for apartment in apartments:
            title = apartment['name']
            property_type = 'apartment' if 'apartment' in apartment['property_type'] else 'hosue'
            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']
            pets_allowed = apartment['pet_friendly']
            landlord_phone = apartment['contact']['phone']
            external_link = apartment['permalink']
            address = apartment['address']['address']+', '+apartment['address']['city']

            datausge = {
                'title':title,
                'property_type':property_type,
                'latitude':latitude,
                'longitude':longitude,
                'pets_allowed':pets_allowed,
                'landlord_phone':landlord_phone,
                'address':address
            }
            yield Request(external_link, callback=self.parseApartment, meta=datausge)

  

    def parseApartment(self, response):

        title = response.meta['title']
        property_type = response.meta['property_type']
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        pets_allowed = response.meta['pets_allowed']
        landlord_phone = response.meta['landlord_phone']
        description = response.css('.cms-content p::text').get()
        address = response.meta['address']
        zipcode = ''
        city = ''
        try:

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


        balcony = True if response.css(".widget.amenities:contains('Balcon')") else False
        dishwasher = True if response.css(".widget.amenities:contains('Dishwasher')") else False
        elevator = True if response.css(".widget.amenities:contains('Elevator')") else False
        parking = True if response.css(".widget.amenities:contains('parking')") else False
        washing_machine = True if response.css(".widget.amenities:contains('Laundry')") else False

        imagesThumb = response.css('.gallery-image a::attr(href)').getall()


        room_count = 1
        rows = response.css('.suites .suite')
        i=1
        for row in rows:
            rex = re.search('\d+',"".join(row.css('.suite-type *::text').getall()))
            if rex:
                room_count=rex[0]
            rex = re.search('\d+',"".join(row.css('.suite-bath *::text').getall()))
            bathroom_count = 1
            if rex:
                bathroom_count=rex[0]
      
            rex = re.search(r'\d+',"".join(row.css('.suite-rent *::text').getall()).replace('$','').replace(',',''))
            rent = 0
            available_date = remove_white_spaces("".join(row.css('.suite-inquire a::text').getall()))
            if 'INQUIRE' in available_date:
                available_date=''
            if 'aiting' in available_date:
                continue
            if rex:
                rent=int(rex[0])
            images = row.css('.suite-photos a::attr(href)').getall() + imagesThumb
            floor_plan_images = row.css('.suite-floorplan a::attr(href)').getall()
            floor_plan_images = [x for x in floor_plan_images if 'icon' not in x]
            external_images_count = len(images)+len(floor_plan_images)
            external_link = response.url+'#'+str(i)
            i+=1
            


            if rent > 0:

                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("title", title)
                item_loader.add_value("description", description)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address", address)
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value('washing_machine', washing_machine)
                item_loader.add_value('pets_allowed', pets_allowed)
                item_loader.add_value('balcony', balcony)
                item_loader.add_value('elevator', elevator)

                item_loader.add_value(
                    "property_type", property_type)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value("available_date", available_date)
                item_loader.add_value(
                    "bathroom_count", bathroom_count)
                item_loader.add_value("images", images)
                item_loader.add_value('floor_plan_images',floor_plan_images)
                item_loader.add_value("external_images_count",
                                    external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "CAD")
                item_loader.add_value('parking', parking)
                item_loader.add_value('dishwasher', dishwasher)

                item_loader.add_value("landlord_name", 'O\'Shanter Development Company Ltd')
                item_loader.add_value(
                    "landlord_email", 'rentals@oshanter.com')
                item_loader.add_value("landlord_phone", landlord_phone)

                item_loader.add_value("position", self.position)
                self.position += 1
                yield item_loader.load_item()