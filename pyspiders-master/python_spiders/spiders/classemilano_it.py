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


class Classemilano_Spider(scrapy.Spider):

    name = 'classemilano'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['classemilano.it']
    start_urls = ['https://www.classemilano.it/immobili/?filter-contract=RENT']

    position = 1

    def parse(self, response):
        apartments = response.css(
            ".col-md-5.col-xs-12.property-box-image a::attr(href)").getall()
        for apartment in apartments:
            yield Request(apartment,
                          callback=self.parseDetails)

    def parseDetails(self, response):

        external_link = response.url
        title = response.css("h1.entry-title::text").get()

        zipcode = ''
        city = ''
        try:
            latitude = float(response.css(
                "#tab-single-property-map::attr(data-latitude)").get())
            longitude = float(response.css(
                "#tab-single-property-map::attr(data-longitude)").get())

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
       

        room_count = response.css(
            ".columns-gap *:contains('Camere')::text").getall()[1]
        try:
            bathroom_count = response.css(
            ".columns-gap *:contains('Bagni')::text").getall()[1]
        except:
            bathroom_count = 1
        
        try:
            rex = re.search(
            r'\d+', response.css(".columns-gap *:contains('mq')::text").getall()[0])
            if rex:
                square_meters = rex[0]
            else:
                square_meters = 0
        except:
            square_meters = 0
        
        swimming_pool = True if response.css(
            ".columns-gap.list-check .yes:contains('Piscina')") else False
        balcony = True if response.css(
            ".columns-gap.list-check .yes:contains('Balcone')") else False
        washing_machine = True if response.css(
            ".columns-gap.list-check .yes:contains('Pulizie finali')") else False
        dishwasher = True if response.css(
            ".columns-gap.list-check .yes:contains('Lavastoviglie')") else False
        pets_allowed = False
        parking = True if response.css(
            ".columns-gap.list-check .yes:contains('Posto auto')") else False
        elevator = True if response.css(
            ".columns-gap.list-check .yes:contains('Ascensore')") else False
        terrace = True if response.css(
            ".columns-gap.list-check .yes:contains('Terrazzo')") else False
        rent = int(extract_number_only(response.css(
            ".price.text-theme.text-right::text").get()))
        description = "".join(response.css(
            ".property-section.property-description p::text").getall())
        description = remove_white_spaces(description)
        images = response.css(
            ".owl-carousel.property-gallery-preview-owl a::attr(href)").getall()

        property_type = 'apartment'


        if rent > 0:

            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("terrace", terrace)

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
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("parking", parking)
            item_loader.add_value(
                "swimming_pool", swimming_pool)
            item_loader.add_value(
                "dishwasher", dishwasher)
            item_loader.add_value(
                "washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'Andrea Lorenzoni')
            item_loader.add_value(
                "landlord_email", 'lorenzoni@classemilano.it')
            item_loader.add_value("landlord_phone", '0276004762')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()


        
