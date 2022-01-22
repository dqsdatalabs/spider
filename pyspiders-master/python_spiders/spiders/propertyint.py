import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class PropertyintSpider(scrapy.Spider):
        
    name = 'propertyint'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.propertyint.net']
    start_urls = ['https://www.propertyint.net/risultati-ricerca/?Lflt11=4337&Lflt11OPE=%3d&Lflt10=4336&Lflt10OPE=%3d']

    position = 1

    def parse(self, response):
   
        
        cards = response.css(".viewcont .boxvetrina")

        for index, card in enumerate(cards):

            position = self.position
            property_type = "apartment"
            card_url = card.css(".center a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
   
            
            PropertyintSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        


    def parseApartment(self, response):

        
        square_meters = response.css(".testiprodottopiccolo:contains('mq') strong::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".testiprodottopiccolo:contains('Camere') strong::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        if room_count == 0:
            room_count = 1 
        else:
            room_count = 1 
            

        rent = response.css(".testiprodotto  h4 strong::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")


        currency = response.css(".testiprodotto  h4 strong::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("div.columns h1::text").get()
        if title:
            title = remove_white_spaces(title)


        latitude = response.css("#lat::attr(value)").get()
        if latitude:
            latitude = latitude.replace(",",".")
        
        longitude = response.css("#lng::attr(value)").get()
        if longitude:
            longitude = longitude.replace(",",".")
        
        
        city = response.css(".testiprodotto  div:contains('Città') + div::text").get()
        if city:
            city = remove_white_spaces(city)
        
        
        address = f"{title} - {city}"
            
    
        description = response.css('.inner-wrap .contenuto .contenuto .pdt20 p::text, .inner-wrap .contenuto .contenuto .pdt20 p strong::text, .inner-wrap .contenuto .contenuto .pdt20 span strong::text, .inner-wrap .contenuto .contenuto .pdt20 p span strong::text, .inner-wrap .contenuto .contenuto .pdt20 p span::text, .inner-wrap .contenuto .contenuto .pdt20 div::text, .inner-wrap .contenuto .contenuto .pdt20 div strong::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.clearing-thumbs li a::attr(href)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)

        furnished = response.css(".testiprodotto  div:contains('Arredamento:') + div::text").get()
        if furnished:
            furnished = remove_white_spaces(furnished).lower()
            if furnished == "arredato":
                furnished = True
            elif "non arredato" in furnished:
                furnished = False
            else:
                furnished = False
        

        
        landlord_email = "MILAN@PROPERTYINT.NET"
        landlord_phone = "+39024980092"
        landlord_name = "Property International srl"
        
        

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
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
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()

