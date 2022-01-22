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

class SunrexSpider(scrapy.Spider):
        
    name = 'sunrex_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['sunrex.ca']
    start_urls = ['https://www.sunrex.ca/find-an-apartment']
    
    position = 1


    def parse(self, response):

            
        cards = response.css(".layout-grid-row-data")

        for index, card in enumerate(cards):
            
            if card.css(".page-price::text").get() in ["Rented", "All Inclusive"]:
                continue

            position = self.position
            
            card_url = card.css(".list-item-datum-title a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            SunrexSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        nextPageUrl = None
        nextPageUrl = response.css(".pager-link[title='next page']::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):
        
        
        if "studio" in response.url:
            property_type = "studio"
        else:
            property_type = "apartment"
            
        external_id = response.css("h2 ~ table:first-of-type td:nth-of-type(1) strong::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)


        room_count = response.css("h6 span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            pattern = re.compile(r'(\d+)? Bed', re.IGNORECASE)
            data_from_regex = pattern.search(room_count)
            if data_from_regex:
                room_count = data_from_regex.group(1)
            else:
                room_count = None

        bathroom_count = response.css("h6 span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            pattern = re.compile(r'(\d+)? Bath', re.IGNORECASE)
            data_from_regex = pattern.search(bathroom_count)
            if data_from_regex:
                bathroom_count = data_from_regex.group(1)
            else:
                bathroom_count = None        

        
        rent = response.css("h2 ~ table:first-of-type td:nth-of-type(3) strong::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = "CAD"

        available_date = response.css("h2 ~ table:first-of-type td:nth-of-type(2) strong::text").get()
        if available_date:
            available_date = remove_white_spaces(available_date)

        title = response.css("head title::text").get()
        if title:
            title = remove_tags(title)   
            title = remove_white_spaces(title)   
            
        address = response.css("head title::text").get().split("-")[-1].strip()
        if address:
            address = remove_tags(address)   
            address = remove_white_spaces(address)   
        
        city = "Winnipeg"
        
        images = response.css('.shout-gallery-galleria a::attr(href)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        
        
        floor_plan_images = response.css('a[title="CLICK TO VIEW SPEC SHEET"] img::attr(src), p span a[rel="noopener"] img::attr(src)').getall()
        floor_plan_images = [response.urljoin(img) for img in floor_plan_images]
 
 
        
        balcony = response.css(".suite-info-block > table:first-of-type p:contains('Balcony')").get()
        if balcony:
            balcony = remove_tags(balcony)
            balcony = remove_unicode_char(balcony)
            if balcony:
                balcony = True
            else:
                balcony = False 
        else:
            balcony = False 

        washing_machine = response.css(".suite-info-block > table:first-of-type p:contains('Laundry')").get()
        if washing_machine:
            washing_machine = remove_tags(washing_machine)
            washing_machine = remove_unicode_char(washing_machine)
            if washing_machine:
                washing_machine = True
            else:
                washing_machine = False 
        else:
            washing_machine = False 
        
        
        
        dishwasher = response.css(".suite-info-block > table:first-of-type p:contains('Dishwasher')").get()
        if dishwasher:
            dishwasher = remove_tags(dishwasher)
            dishwasher = remove_unicode_char(dishwasher)
            if dishwasher:
                dishwasher = True
            else:
                dishwasher = False 
        else:
            dishwasher = False 
        
        
        
        landlord_phone = response.css(".suite-info-block a[href*=tel]::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)
            landlord_phone = landlord_phone.replace("-","")
        
        landlord_email = response.css(".suite-info-block a[href*=mailto]::attr(href)").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
            landlord_email = remove_white_spaces(landlord_email).split(":")[-1]
        
        landlord_name = response.css(".suite-info-block h4:contains('LEASING INQUIRIES')::text, .suite-info-block h4:contains('leasing inquiries')::text, .suite-info-block h4:contains('BUILDING MANAGER')::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
            landlord_name = remove_white_spaces(landlord_name).split(" - ")[-1]

        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
