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


class Agenzia_domus_Spider(scrapy.Spider):

    name = 'agenzia_domus'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        for i in range(1, 10):
            url = f'https://www.agenzia-domus.com/it/condizioni_contrattuali/affitto/page/{i}/'
            yield Request(url, callback=self.parseDetails,dont_filter=True)

    def parseDetails(self, response):

        
        apartments = response.css('.property_div.main')
        
        for apartment in apartments:
            title = apartment.css('.property_title a::text').get()
            if 'negozio' in title.lower() or 'ufficio' in title.lower():
                continue
            external_link = apartment.css(
                '.property_title a::attr(href)').get()
            rent = 0
            rex = re.search(r'\d+', apartment.css('.property_price::text').get().replace('.', ''))
            if rex:
                rent = int(rex[0])
            city = apartment.css('.property_city::text').get()

            datausge = {
                'title': remove_white_spaces(title),
                'rent': rent,
                'city': city
            }
            yield Request(external_link, callback=self.parseApartment, meta=datausge)

    def parseApartment(self, response):
        title = response.meta['title']
        rent = response.meta['rent']
        city = response.meta['city']
        address = city
        description = remove_white_spaces(
            "\n".join(response.css('.wpp_the_content p::text').getall()))
        if not 'ppartamento' in description.lower():
            return
        external_id = response.css('.propriet_domus_id .value::text').get()
        landlord_phone = response.css('.propriet_phone .value::text').get()
        room_countTxt = response.css('.propriet_bedrooms .value::text').get()
        rex = re.findall(r'\d+',room_countTxt)
        room_count=0
        for i in rex:
            room_count+=int(i)
        if room_count==0:
            room_count = '1'
        bathroom_count = response.css('propriet_bathrooms .value::text').get()
        if not bathroom_count:
            bathroom_count='1'
        energy_label = response.css('.propriet_energ .value::text').get()
        images = response.css('.right a::attr(href)').getall()
        del images[-1]
        #images = [x.replace(re.search(r'-\d+x\d+',x)[0],'') for x in images if not 'Skype' in x]
        balcony = True if 'balcon' in description else False
        square_meters = 0
        if response.css('.propriet_area .value::text').get():
            square_meters = response.css('.propriet_area .value::text').get()

        parking = True if 'parcheggi' in description else False

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value('energy_label', energy_label)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value('balcony', balcony)

            item_loader.add_value(
                "property_type", 'apartment')
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value('parking', parking)

            item_loader.add_value("landlord_name", 'AGENZIA DOMUS')
            item_loader.add_value(
                "landlord_email", 'info@agenzia-domus.it')
            item_loader.add_value("landlord_phone", landlord_phone)

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
