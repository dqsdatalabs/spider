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


class Hollyburn_Spider(scrapy.Spider):

    name = 'hollyburn'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1
    anchor = 1

    stopParsing = False

    def start_requests(self):
        start_url = 'https://www.hollyburn.com/wp-json/buildings-api/v2/get-unfiltered-buildings/'
        yield Request(
            start_url, headers={'token': '3Vflt8ly2BRDzFI1XMbbtuJC0eRsulCk'}, callback=self.parse)

    def parse(self, response):

        apartments = json.loads(response.text)
        for apartment in apartments:

            title = apartment['name']

            external_id = apartment['external_ID']
            landlord_phone = apartment['phone']
            landlord_email = apartment['email']
            external_link = apartment['link']
            address = apartment['full_address']
            city = apartment['city']
            longitude = apartment['longitude']
            latitude = apartment['latitude']
            zipcode = apartment['zipcode']

            features = apartment['suite_features'] + \
                apartment['building_amenities']
            dishwasher = True if 'Dishwasher' in features else False
            balcony = True if 'Balcony' in features else False
            furnished = False if 'Unfurnished' in features else True
            washing_machine = True if 'Laundry' in features else False
            swimming_pool = True if 'Pool' in features else False

            buildingImages = apartment['building_albums']
            buildingImages = [x.replace('-600x400', '')
                              for x in buildingImages]

            suites = apartment['suites']

            dataUsage = {
                'suites':suites,
                'title': title,
                "external_id": external_id,
                "external_link": external_link,
                "city": city,
                "address": address,
                "zipcode": zipcode,
                "furnished": furnished,
                "longitude": longitude,
                "latitude": latitude,
                "swimming_pool": swimming_pool,
                'landlord_phone': landlord_phone,
                'landlord_email': landlord_email,
                "washing_machine": washing_machine,
                "balcony": balcony,
                "dishwasher": dishwasher,
                'buildingImages': buildingImages,
            }
            yield Request(external_link, meta=dataUsage, callback=self.parse_description)
            print('')
                

    def parse_description(self, response):

        title = response.meta['title']
        external_id = response.meta["external_id"]
        external_link = response.meta["external_link"]
        city = response.meta["city"]
        address = response.meta["address"]
        zipcode = response.meta["zipcode"]
        furnished = response.meta["furnished"]
        longitude = response.meta["longitude"]
        latitude = response.meta["latitude"]
        swimming_pool = response.meta["swimming_pool"]
        landlord_phone = response.meta['landlord_phone']
        landlord_email = response.meta['landlord_email']
        washing_machine = response.meta["washing_machine"]
        balcony = response.meta["balcony"]
        dishwasher = response.meta["dishwasher"]
        bathroom_count = 1
        buildingImages = response.meta['buildingImages']

        print(external_link)

        description = remove_white_spaces("".join(response.css(
            '.grid.grid--flex.grid--extra-thick p::text').getall()))
        parking = True if response.css(".building-amenities *:contains('Underground Parking')") or response.css(
            ".building-amenities *:contains('Surface Parking')") else False

        
        suites = response.meta['suites']
        if not suites:
            return
        self.anchor=1
        for unit in suites:
            
            url = external_link[:len(external_link)-2]+ '#'+str(self.anchor)
            self.anchor+=1
            
            square_meters = sq_feet_to_meters(unit['size'])

            available_date = unit['available_date']
            rent = int(unit['starting_price'])
            floor_plan_images = [unit['pdf_floorplan']]

            room_count = unit['type'][0] if 'Bedroom' in unit['type'] else '1'
            property_type = 'apartment' if 'Bedroom' in unit['type'] else unit['type']
            images = json.loads(unit['images']) + buildingImages
        
        
            if rent > 0:

                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", url)
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

                item_loader.add_value("furnished", furnished)

                item_loader.add_value("square_meters", int(int(square_meters)*10.764))
                item_loader.add_value("floor_plan_images", floor_plan_images)
                item_loader.add_value("dishwasher", dishwasher)

                item_loader.add_value('available_date', available_date)
                item_loader.add_value(
                    "property_type", property_type)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value(
                    "swimming_pool", swimming_pool)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "CAD")
                item_loader.add_value("parking", parking)
                item_loader.add_value("balcony", balcony)
                item_loader.add_value(
                    "washing_machine", washing_machine)
                item_loader.add_value("landlord_name", 'hollyburn')
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("landlord_email", landlord_email)

                item_loader.add_value("position", self.position)
                self.position += 1
                yield item_loader.load_item()
