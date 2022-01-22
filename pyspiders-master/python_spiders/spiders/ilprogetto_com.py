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


class Ilprogetto_Spider(scrapy.Spider):

    name = 'ilprogetto'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    start_url = []

    position = 1

    stopParsing = False

    def start_requests(self):
        for i in range(1, 4):
            link = f'https://ilprogetto.com/advanced-search/page/{i}/?cerca-riferimento&parola-chiave&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D=appartamenti'
            yield Request(link, callback=self.parse, dont_filter=True)
        for i in range(1, 4):
            link = f'https://ilprogetto.com/advanced-search/page/{i}/?cerca-riferimento&parola-chiave&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D=villa-singola'
            yield Request(link, callback=self.parse, dont_filter=True)
        for i in range(1, 4):
            link = f'https://ilprogetto.com/advanced-search/page/{i}/?cerca-riferimento&parola-chiave&filter_search_action%5B0%5D=affitto&filter_search_type%5B0%5D=villa-bifamiliare'
            yield Request(link, callback=self.parse, dont_filter=True)

    def parse(self, response):

        yield Request(response.url, dont_filter=True,
                      headers={"Accept": "*/*",
                               "Accept-Encoding": "gzip, deflate, br"},
                      callback=self.parseDetails)

    def parseDetails(self, response):

        apartments = response.css('.property_listing .property-unit-information-wrapper h4')

        for apartment in apartments:
            title = remove_white_spaces(apartment.css('a::text').get())
            if 'Appartamenti' in title or 'Villa' in title:
                property_type = 'apartment' if 'ppartamenti' in title else 'house'
                external_link = apartment.css('a::attr(href)').get()

                yield Request(external_link,
                              meta={'title': title,
                                    'property_type': property_type},
                              callback=self.parseApartment)

    def parseApartment(self, response):

        rent = 0
        try:

            rent = int(extract_number_only(response.css(
                '.notice_area .price_area *::text').get()))
        except:
            rent = 0

        if rent == 0:
            return

        title = response.css('.entry-title.entry-prop::text').get()
        property_type = response.meta['property_type']
        rex = re.search(r'Rif. (\d+)', title)

        external_id = str(rex.groups()[0])
        square_meters = 0
        info = response.css('.overview_element *::text').getall()
        room_count = 1
        bathroom_count = 1

        for i in info:
            if 'Camere' in i:
                room_count = int(i[0])
            if 'Bagni' in i:
                bathroom_count = int(i[0])
            if '.00 m' in i:
                x = i.replace('.00 m', '').replace(',', '')
                square_meters = int(x)

        description = remove_white_spaces(response.css(
            '.single-content.listing-content .wpestate_property_description p *::text').get())

        images = response.css(
            '#carousel-property-page-header .item .propery_listing_main_image::attr(style)').getall()
        images = [x.replace('background-image:url(', '').replace(')', '')
                  for x in images]

        floor_plan_images = response.css(
            '.single-content.listing-content .wpestate_property_description p  img::attr(src)').getall()
        floor_plan_images = [
            x for x in floor_plan_images if 'icone' not in x and len(x) > 0]
        external_images_count = len(images) + len(floor_plan_images)

        zipcode = ''
        address = ''
        city = ''
        try:

            rex = re.findall(
                r'latitude":"(\d+.\d+)|longitude":"(\d+.\d+)', response.css('#wpestate_mapfunctions_base-js-extra::text').get())

            if rex:
                longitude = rex[1][1]
                latitude = rex[0][0]

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

        info = response.css('.panel-body .listing_detail *::text').getall()
        furnished = False
        elevator = False
        energy_label = ''
        utilities = 0
        for idx, w in enumerate(info):
            if 'energetica:' in w:
                energy_label = info[idx+1][1]
            if 'Ascensore' in w:
                elevator = True if 'S' in info[idx+1] else False
            if 'ARREDATO' in w:
                furnished = True
            if 'Spese Condominiali' in w:
                try:
                    utilities = int(info[idx+1].replace(' ','').replace(',00',''))
                except:
                    utilities = 0
                

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value(
                "square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  external_images_count)
            item_loader.add_value('floor_plan_images', floor_plan_images)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value("furnished", furnished)
            item_loader.add_value('energy_label', energy_label)

            item_loader.add_value("elevator", elevator)
            item_loader.add_value("landlord_name", 'ilprogetto')
            item_loader.add_value("landlord_email", 'info@ilprogetto.com')
            item_loader.add_value("landlord_phone", '051 377757')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
