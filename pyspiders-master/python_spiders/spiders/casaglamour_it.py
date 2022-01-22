import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
import math
from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json


class casaglamour_Spider(scrapy.Spider):

    name = 'casaglamour_net'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        for i in range(1, 6):
            start_url = f"https://www.casaglamour.net/ita/immobili?order_by=&rental=1&categories_id=&rental=1&property_type_id=1001&page={i}"
            yield Request(start_url, callback=self.parse)

    def parse(self, response):

        apartments = response.css('a.detail')
        for apartment in apartments:
            url = apartment.css('::attr(href)').get()
            external_id = apartment.css(
                ".value:contains(Rif)::text").get().replace('Rif. ', '')
            yield Request(url, meta={
                'external_id': external_id
            }, callback=self.parseApartment)

    def parseApartment(self, response):

        description = remove_white_spaces(
            "".join(response.css('p.description::text').getall()))
        property_type = 'apartment'
        title = response.css("meta[property='og:title']::attr(content)").get()
        external_id = response.meta["external_id"]
        rent = '0'
        try:
            rent = re.search(
                r'\d+', response.css(".section li:contains('Prezzo') b::text").get().replace('.', ''))[0]
        except:
            return

        square_meters = response.css(
            ".section li:contains('MQ') b::text").get()
        energy_label = response.css(
            ".section li:contains(Energ) b::text").get()
        
        utilities = 0
        try:
            utilities = int(int(re.search(
                r'\d+', response.css(".section li:contains(Spese) b::text").get().replace('.', ''))[0])/12)
        except:
            utilities = 0

        if 'VA' in energy_label or 'NA' in energy_label or 'ND' in energy_label:
            energy_label = ''

        bathroom_count = response.css(
            ".section li:contains('Bagni') b::text").get()
        if not bathroom_count:
            bathroom_count = '1'
        room_count = '1'
        try:
            room_count = re.search(
                r'\d+', response.css(".section li:contains('ocali') b::text").get())[0]
        except:
            room_count = '1'

        furnished = True if 'Ottime finiture' in description else False
        parking = True if response.css(
            ".section li:contains('archeggio')") or 'parking' in description.lower() or response.css(".section li:contains('rredato')") else False
        terrace = True if response.css(
            ".section li:contains('errazzo')") or 'errazz' in description else False
        elevator = True if response.css(".section li:contains('scensore')") or'scensore' in description else False
        balcony = True if 'balcon' in description or response.css(".section li:contains('alcone')") else False
        swimming_pool = True if 'piscina' in description else False
        washing_machine = True if response.css(".section li:contains('avatrice')") else False
        dishwasher = True if response.css(".section li:contains('avastovigli')") else False
        pets_allowed = True if response.css(".section li:contains('Animali Ammessi')") else False

        images = response.css('img.sl::attr(src)').getall()
        floor_plan_images = response.css(
            '.planimetries_list a::attr(href)').getall()

        rex = re.findall(r'-?\d+\.\d+', "".join(response.css(
            "#content4 #tab-map script:contains(LatLng) *::text").getall()))
        zipcode = ''
        city = ''
        latitude = ''
        longitude = ''
        address = ''
        if rex:
            latitude = rex[0]
            longitude = rex[1]
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)

        if city == '':
            address = title[:title.find(' | ')]
            address = address.replace('Appartamento affitto a ', '')
            city=address

        if (int(rent) > 0):

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)

            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value('description', description)

            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)

            item_loader.add_value("landlord_name", 'casaglamour')
            item_loader.add_value(
                "landlord_email", 'casaglamourgenova@gmail.com')
            item_loader.add_value("landlord_phone", '+39 392 4416009')
            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
