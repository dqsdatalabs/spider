# -*- coding: utf-8 -*-
# Author: Ahmed Atef

import scrapy
from scrapy import Request, FormRequest
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

class CercocasabariSpider(scrapy.Spider):
        
    name = 'cercocasabari'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.cercocasabari.it']
    start_urls = ['https://www.cercocasabari.it/immobili/?filter-property-type=5&filter-contract=RENT&filter-location=&filter-rooms=&filter-price-from=&filter-price-to=']

    position = 1
    
    def parse(self, response):


        cards = response.css("div.property-container")

        for index, card in enumerate(cards):

            position = self.position
            property_type = "apartment"
            card_url = card.css("h3 a::attr(href)").get()
    
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            CercocasabariSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)


        nextPageUrl = response.css("a.next::attr(href)").get()


        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)






    def parseApartment(self, response):
        external_id = response.css("link[rel='shortlink']::attr(href)").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)

        square_meters = response.css(".property-overview dt:contains('Superficie') + dd::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".property-overview dt:contains('Locali') + dd::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1
            
        rent = response.css(".property-overview dt:contains('Prezzo') + dd::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".property-overview dt:contains('Prezzo') + dd::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h1.property-title::text").get()
        if title:
            title = remove_white_spaces(title)
        
            
            
        zona = response.css(".property-overview dt:contains('Zona') + dd a::text").getall()
        zona = " - ".join(zona)
        if zona:
            zona = remove_white_spaces(zona)    
            
        city = zona.split(" - ")[0]
        address = zona
            
        
        latitude = response.css("#simple-map::attr(data-latitude)").get()
        longitude = response.css("#simple-map::attr(data-longitude)").get()
        
        if latitude:
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']    
        else:
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            longitude = str(longitude)
            latitude = str(latitude)
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']  
    
        description = response.css('.property-description p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.msnry_item a::attr(href)').getall()
        external_images_count = len(images)
        
    
        
        landlord_email = "francone.consulente@gmail.com"
        landlord_phone = "3291570430"
        landlord_name = "Studio Immobiliare Nanna"

        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
