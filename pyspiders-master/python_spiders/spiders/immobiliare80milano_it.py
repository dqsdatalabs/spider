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

class Immobiliare80milanoSpider(scrapy.Spider):
        
    name = 'immobiliare80milano'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliare80milano.it']
    start_urls = ['https://immobiliare80milano.it/annunci-immobiliari/affitto/']

    position = 1
    def parse(self, response):

            
        cards = response.css(".elencoann")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".anteprimaann a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            Immobiliare80milanoSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".PaginatoreSel + .PaginatoreLink a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)



    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.url.split("/")[-2]


        square_meters = response.css(".sotto li:contains('Superficie')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".sotto li:contains('Locali')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".sotto li:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".sotto li:contains('Canone')::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".sotto li:contains('Canone')::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        

        title = response.css("#side h1::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address_all = response.css(".dove::text").getall()
        
        address = " ".join(address_all)
        if address:
            address = remove_white_spaces(address)
            
        city = remove_white_spaces(address_all[1])
        city = city.split("Si trova in: ")[1].split(",")[0]
            
        zipcode = remove_white_spaces(address_all[-1])
        if "-" in zipcode:
            zipcode = zipcode.split("- ")[1] 
        else:
            zipcode = None
        

    
        description = response.css(".descann::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.fotorama img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        floor = response.css(".sotto li:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor).split("Piano: ")[1]
         
              
        parking = response.css(".sotto li:contains('Box')::text").get()
        if parking:
            parking = parking.split(": ")[1]
            if "si" in parking:
                parking = True
            else:
                parking = False  
        
        terrace = response.css(".sotto li:contains('Terrazzo')::text").get()
        if terrace:
            terrace = terrace.split(": ")[1]
            if "si" in terrace:
                terrace = True
            elif terrace == "0":
                terrace = False
            else:
                terrace = False      
        
        
        landlord_email = "piola@immobiliare80milano.it"
        landlord_phone = "+393287308178"
        landlord_name = "IMMOBILIARE 80 MILANO"


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
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
