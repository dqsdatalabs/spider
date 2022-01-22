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

class GruppomdSpider(scrapy.Spider):
        
    name = 'gruppomd'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.gruppomd.com']

    position = 1
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.gruppomd.com/cerca-immobili/residenziali-in-affitto/',
            'property_type': 'apartment'},
            {'url': 'https://www.gruppomd.com/cerca-immobili/ville-e-casali-in-affitto/',
            'property_type': 'house'},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta={'property_type': url.get('property_type')})

    def parse(self, response):
        
        
        
        cards = response.css(".item-listing-wrap")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = response.meta['property_type']
            
            card_url = card.css(".item-header a.hover-effect::attr(href)").get() 
                            
        
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            
            GruppomdSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".page-item .page-link[aria-label='Next']::attr(href)").get()
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      






    def parseApartment(self, response):
        
        external_id = response.css(".block-title-wrap div:contains('Proprietà')::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css(".detail-wrap ul li strong:contains('Superficie') + span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".detail-wrap ul li strong:contains('Local') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".detail-wrap ul li strong:contains('Bagn') + span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css(".detail-wrap ul li strong:contains('Prezzo') + span::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
            
        currency = response.css(".detail-wrap ul li strong:contains('Prezzo') + span::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".page-title h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        address = response.css(".item-address::text").get()
        if address:
            address = remove_white_spaces(address)
                        
        city = response.css(".detail-city span::text").get()
        if city:
            city = remove_white_spaces(city)
        
        zipcode = response.css(".detail-zip span::text").get()
        if zipcode:
            zipcode = remove_white_spaces(zipcode)
        else:
            zipcode = extract_number_only(address) or ""
        
        script_map = response.css("#houzez-single-property-map-js-extra::text").get()
        if script_map:
            pattern = re.compile(r',"lat":"(\d*\.?\d*)","lng":"(\d*\.?\d*)",')
            x = pattern.search(script_map)
            latitude = x.groups()[1]
            longitude = x.groups()[0] 
        
        description = response.css('.property-description-wrap .block-content-wrap p::text').getall()        
        description = [re.sub(r'(Cell.? \d*)|(Tel.? \d*)|(info@gruppomd.com)|(www.gruppomd.com)|(\.+\d*\.+\d+)', "", des) for des in description]
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('#property-gallery-js img::attr(src)').getall()
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]
        external_images_count = len(images)
        
 
        energy_label = response.css(".class-energy-list li strong:contains('energetica') + span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css(".block-content-wrap ul li strong:contains('Spese') + span::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities).split(",")[0]
            utilities = extract_number_only(utilities)
        
        floor = response.css(".block-content-wrap ul li strong:contains('Piano') + span::text").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css(".block-content-wrap ul li strong:contains('Ascensore') + span::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "no":
                elevator = False
            elif elevator == "si":
                elevator = True
            else:
                elevator = False

            
        balcony = response.css(".block-content-wrap ul li strong:contains('Balcone') + span::text").get()
        if balcony:
            balcony = remove_white_spaces(balcony).lower()
            if balcony == "no":
                balcony = False
            elif balcony == "si":
                balcony = True
            else:
                balcony = False  
        
        furnished = "arredato" in description
        if furnished:
            furnished = True  
        else:
            furnished = False  
        
        landlord_email = "info@gruppomd.com"
        landlord_phone = "0677250085"
        landlord_name = "Gruppo MD"
                
        

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
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("floor", floor)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
