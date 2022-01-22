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


class Plana_Spider(scrapy.Spider):

    name = 'plana'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['plana.pro']

    position = 1

    stopParsing = False

    def start_requests(self):

        start_url = 'https://www.plana.pro/shortTerm/ajax_requests.php'
        formdata = {
            'page': '1',
            'action': 'load_records'
        }
        for i in range(1, 50):
            formdata['page'] = str(i)
            if self.stopParsing:
                break
            yield FormRequest(start_url,
                              formdata=formdata,
                              callback=self.parseDetails)

    def parseDetails(self, response):

        apartments = response.css('#Detail_content')

        if len(apartments) > 0:
            for apartment in apartments:
                external_link = 'https://www.plana.pro/' + \
                    apartment.css('#detail_para a::attr(href)').get()
                rex = re.search(r'id=(\d+)', external_link)
                external_id = str(rex.groups()[0])
                

                title = apartment.css('#Detail_content h2::text').get()
                address = re.sub(r'#\d+ - ', "", title) + ',canada'

                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()
                zipcode = ''
                city = ''
                try:
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
                    longitude = ""
                    latitude = ""

                if address == "":
                    address = title

                info = apartment.css("#detail_para *::text").getall()
                room_count = 1
                bathroom_count = 1
                square_meters = 0
                swimming_pool = False
                balcony = False
                washing_machine = False
                pets_allowed = False
                parking = False
                elevator = False
                furnished = False
                rent = 0
                for i in info:
                    if 'Bedroom' in i:
                        room_count = int(i[0])
                    if 'Bathroom' in i:
                        bathroom_count = int(i[0])
                    if 'Sq' in i:
                        square_meters = int(
                            float(re.findall(r'\d+', i)[0])/10.764)
                    if 'Furnished: Yes' in i:
                        furnished = True
                    if '$' in i:
                        rex = re.findall('\d+,\d+', i)[0]
                        rent = int(rex.replace(',', ''))

                    if 'levator' in i:
                        elevator = True
                    if 'Pool' in i:
                        swimming_pool = True
                    if 'Balcony' in i:
                        balcony = True
                    if 'Washer' in i:
                        washing_machine = True
                    if 'Pet' in i:
                        pets_allowed = True
                    if 'Parking' in i:
                        parking = True

                property_type = 'apartment'

                dataUsage = {
                    "property_type": property_type,
                    'title': title,
                    "external_id": external_id,
                    "external_link": external_link,
                    "city": city,
                    "address": address,
                    "zipcode": zipcode,
                    "furnished": furnished,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "rent": rent,
                    'elevator': elevator,
                    'swimming_pool': swimming_pool,
                    'balcony': balcony,
                    'washing_machine': washing_machine,
                    'pets_allowed': pets_allowed,
                    'parking': parking
                }

                yield Request(external_link, meta=dataUsage,
                              callback=self.parseApartment)
        else:
            self.stopParsing = True

    def parseApartment(self, response):

        description = "".join(response.css(
            '.main_content .column2 *::text').getall()).replace('-', '\n')
        description = remove_white_spaces(description)
        images = response.css('.galleria a::attr(href)').getall()
        rex = re.search(r'Available (.+)',
                        response.css(".price_list *::text").get())
        if rex:
            available_date = rex.groups()[0]
        else:
            available_date = ""
        images = ['https://www.plana.pro/' + x for x in images]

        if response.meta['rent'] > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta["external_id"])
            item_loader.add_value("title", response.meta["title"])
            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value('available_date', available_date)
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            item_loader.add_value(
                "property_type", response.meta['property_type'])
            item_loader.add_value(
                "square_meters", response.meta['square_meters'])
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value(
                "bathroom_count", response.meta['bathroom_count'])
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", "CAD")

            item_loader.add_value("furnished", response.meta['furnished'])

            item_loader.add_value("balcony", response.meta['balcony'])

            item_loader.add_value("elevator", response.meta['elevator'])
            item_loader.add_value("parking", response.meta['parking'])

            item_loader.add_value(
                "swimming_pool", response.meta['swimming_pool'])
            item_loader.add_value(
                "pets_allowed", response.meta['pets_allowed'])
            item_loader.add_value(
                "washing_machine", response.meta['washing_machine'])
            item_loader.add_value("landlord_name", 'plana')
            item_loader.add_value("landlord_email", 'buildings@plana.pro')
            item_loader.add_value("landlord_phone", '604-441-8834')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
