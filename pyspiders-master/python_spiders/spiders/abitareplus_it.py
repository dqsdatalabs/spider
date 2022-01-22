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


class Abitareplus_Spider(scrapy.Spider):

    name = 'abitareplus'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['abitareplus.it']
    start_urls = [
        'https://www.abitareplus.it/ricerca-avanzata/?status=in-affitto-brescia&type=residenziale']

    position = 1

    def parse(self, response):

        apartments = response.css('.rh_list_card__details h3')
        for apartment in apartments:
            url = apartment.css('a::attr(href)').get()
            title = apartment.css('a::text').get()
            yield Request(url, meta={
                'title': title
            },
                callback=self.parseDetails)

    def parseDetails(self, response):

        external_id = remove_white_spaces(response.css('.id::text').get())
        title = response.meta['title']

        zipcode = ''
        city = ''
        address = remove_white_spaces(response.css(
            ".rh_page__property_address::text").get())
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

        if address == "":
            address = title

        room_count = int(response.css(
            '.rh_property__row span.figure::text').getall()[0])
        try:
            bathroom_count = response.css(
                '.rh_property__row span.figure::text').getall()[1]
        except:
            bathroom_count = 1

        parking = True
        for i in range(2, 4):
            try:
                rex = re.search(
                    r'\d+', response.css('.rh_property__row span.figure::text').getall()[3])
                if rex:
                    square_meters = rex[0]
                else:
                    square_meters = 0
            except:
                rex = re.search(
                    r'\d+', response.css('.rh_property__row span.figure::text').getall()[2])
                if rex:
                    square_meters = rex[0]
                else:
                    square_meters = 0
                parking = False

            if not parking:
                try:
                    rex = re.search(
                        r'\d+', response.css('.rh_property__row span.figure::text').getall()[2])
                    if rex:
                        parking = True if int(rex[0]) > 0 else False
                except:
                    parking = False

        rent = int(extract_number_only(response.css(
            ".price:contains('€')::text").get()).replace('€', "").replace('.00', ""))

        description = "".join(response.css(".rh_content p *::text").getall())
        description = remove_white_spaces(description)

        rex = re.search(r'ENERGETICA (.)', description)
        if rex:
            energy_label = str(rex.groups()[0])
        else:
            energy_label = ''

        property_type = 'apartment'

        images = response.css('.slides a::attr(href)').getall()

        if rent > 0 and rent < 10000:

            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value("title", title)
            item_loader.add_value('energy_label', energy_label)
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
                                  len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

            item_loader.add_value("parking", parking)
            item_loader.add_value("landlord_name", 'abitareplus')
            item_loader.add_value(
                "landlord_email", 'info@abitareplus.it')
            item_loader.add_value("landlord_phone", '030 8360 840')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
