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
from w3lib.html import remove_tags

class SterlingrealtygroupSpider(scrapy.Spider):
        
    name = 'sterlingrealtygroup_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['sterlingrealtygroup.com']
    start_urls = ['https://sterlingrealtygroup.com/search-results/?use_radius=on&radius=20&status%5B%5D=for-rent&type%5B%5D=residential&type%5B%5D=apartment&type%5B%5D=condo&type%5B%5D=duplex&type%5B%5D=mobile-home&type%5B%5D=single-house&type%5B%5D=studio&type%5B%5D=townhouse&min-price=100&max-price=10000']
    
    position = 1

    def parse(self, response):


        cards = response.css(".card-deck .card")

        for index, card in enumerate(cards):
            
            if card.css(".page-price::text").get() in ["Rented", "All Inclusive"]:
                continue

            position = self.position
            
            card_url = card.css("a.btn-item::attr(href)").get()

                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            SterlingrealtygroupSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination li a[rel=Next]::attr(href)").get()

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "house"
            
        external_id = response.css("link[rel=shortlink]::attr(href)").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)

        room_count = response.css("strong:contains('Bedroom:') + span::text,strong:contains('Bedrooms:') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css("strong:contains('Bathroom:') + span::text,strong:contains('Bathrooms:') + span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)       

        
        rent = response.css("strong:contains('Price:') + span::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = "CAD"
            

        title = response.css(".page-title h1::text").get()
        if title:
            title = remove_tags(title)   
            title = remove_white_spaces(title)   
            
        address = response.css(".page-title-wrap .container address.item-address::text").get()
        if address:
            address = remove_tags(address)   
            address = remove_white_spaces(address)   
        
        city = response.css("strong:contains('City') + span::text").get()
        if city:
            city = remove_white_spaces(city)
        
        zipcode = response.css("strong:contains('Zip/Postal Code') + span::text").get()
        if zipcode:
            zipcode = remove_white_spaces(zipcode)
        
        script_map = response.css("#houzez-single-property-map-js-extra::text").get()
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r',"lat":"(\d*\.?\d*)?","lng":"(\-?\d*\.?\d*)?",')
            data_from_regex = pattern.search(script_map)
            latitude = data_from_regex.group(1)
            longitude = data_from_regex.group(2)
        
        description = response.css("#property-description-wrap .block-content-wrap ::text").getall()
        description = " ".join(description)
        if description:
            description = remove_white_spaces(description)
        
        images = response.css('#property-gallery-js img::attr(src)').getall()
        external_images_count = len(images)
        
        
        
        landlord_phone = response.css("#sidebar .show-on-click::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)
            landlord_phone = landlord_phone.replace("-","")
        
        landlord_email = response.css("#sidebar input[name=target_email]::attr(value)").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
        
        
        landlord_name = response.css("#sidebar .agent-details .agent-name::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
            
        if room_count and rent:
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
                item_loader.add_value("property_type", property_type)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value("bathroom_count", bathroom_count)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_email", landlord_email)
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("position", response.meta['position'])

                yield item_loader.load_item()
                
        elif not room_count:
            
                kinds = response.css("#property-sub-listings-wrap .block-content-wrap table tbody tr")

                for item in kinds:
                            
                    room_count = item.css("td[data-label=Beds]::text").getall()
                    if room_count:
                        room_count = " ".join(room_count)
                        room_count = remove_white_spaces(room_count)
                        room_count = extract_number_only(room_count)

                    bathroom_count = item.css("td[data-label=Baths]::text").getall()
                    if bathroom_count:
                        bathroom_count = " ".join(bathroom_count)
                        bathroom_count = remove_white_spaces(bathroom_count)
                        bathroom_count = extract_number_only(bathroom_count)       

                    
                    rent = item.css("td[data-label=Price]").get()
                    if rent:
                        rent = extract_number_only(rent).replace(".","")
                    else:
                        rent = None
                
                
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
                    item_loader.add_value("property_type", property_type)
                    item_loader.add_value("room_count", room_count)
                    item_loader.add_value("bathroom_count", bathroom_count)
                    item_loader.add_value("images", images)
                    item_loader.add_value("external_images_count", external_images_count)
                    item_loader.add_value("rent", rent)
                    item_loader.add_value("currency", currency)
                    item_loader.add_value("landlord_name", landlord_name)
                    item_loader.add_value("landlord_email", landlord_email)
                    item_loader.add_value("landlord_phone", landlord_phone)
                    item_loader.add_value("position", response.meta['position'])

                    yield item_loader.load_item()
