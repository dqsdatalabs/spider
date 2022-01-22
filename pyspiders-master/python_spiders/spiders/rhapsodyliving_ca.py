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
import math

class RhapsodylivingSpider(scrapy.Spider):
        
    name = 'rhapsodyliving_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['rhapsodyliving.ca']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=250&auth_token=sswpREkUtyeYjeoahA2i&city_id=3418&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=5500&min_sqft=0&max_sqft=10000&only_available_suites=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=1000&neighbourhood=&amenities=&promotions=&city_ids=3133%2C845%2C3418&pet_friendly=&offset=0&count=false']
    
    
    position = 1
              
    def parse(self, response):

        
        jsonResponse = response.json()
        

        for index, card in enumerate(jsonResponse):

            position = self.position
            card_url= card["name"].lower().replace(" ","-")
            card_url_link = f'https://www.rhapsodyliving.ca/properties/{card_url}'

            landlord_phone = card['contact']['phone']
            landlord_email = card['contact']['email']
            landlord_name = card['contact']['name']
            
            address = f"{card['address']['address']}, {card['address']['city']}, {card['address']['province']}, {card['address']['postal_code']}, {card['address']['country']}"
            
            city = card['address']['city']
            zipcode = card['address']['postal_code']
            pets_allowed = card['pet_friendly']
            
            parking = card['parking']
            Y = False
            for v in parking.values():
                if v != None:
                    Y = True
                    break
            parking = Y
            
            available_date = card['min_availability_date']
            
            if "custom_fields" in card and "property_description" in card['custom_fields']:
                description = card['custom_fields']['property_description']
            else:
                description = None
            
            latitude = card['geocode']['latitude']
            longitude = card['geocode']['longitude']
            
            
            external_id = card['id']
            
            dataUsage = {
                "position": position,
                "card_url_link":card_url_link,
                "landlord_phone":landlord_phone,
                "landlord_email":landlord_email,
                "landlord_name":landlord_name, 
                "address":address, 
                "city":city, 
                "zipcode":zipcode, 
                "pets_allowed":pets_allowed, 
                "parking":parking, 
                "available_date":available_date, 
                "description":description, 
                "latitude":latitude, 
                "longitude":longitude, 
                "external_id":external_id, 
            }

            RhapsodylivingSpider.position += 1
            yield Request(  card_url_link, 
                            method="get",
                            callback=self.parseApartment, 
                            dont_filter=True, 
                            meta=dataUsage
                            )

    def parseApartment(self, response):

        
        currency = "CAD"
        property_type = "apartment"
        external_link = response.meta['card_url_link']
        external_id_root = response.meta['external_id']
        description = response.meta['description']  
        landlord_phone = response.meta['landlord_phone']
        landlord_email = response.meta['landlord_email']
        landlord_name = response.meta['landlord_name']
        position = response.meta['position']
        address = response.meta['address']
        city = response.meta['city']
        zipcode = response.meta['zipcode']
        pets_allowed = response.meta['pets_allowed']
        parking = response.meta['parking']
        available_date = response.meta['available_date']
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']

            
        title = response.css("head title::text").get()
        
        
        images = []
        images = response.css('.gallery .gallery-image .cover::attr(style)').getall()
        images = [img.replace("background-image:url('","").replace("');","") for img in images]
        external_images_count = len(images)
        
        elevator = response.css(".amenity:contains('Elevators')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  
            
        dishwasher = response.css(".amenity:contains('Dishwasher')::text").get()
        if dishwasher:
            dishwasher = True
        else:
            dishwasher = False  
            
        washing_machine = response.css(".amenity:contains('Washer')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False 
             
        balcony = response.css(".amenity:contains('Balcon')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
            
        terrace = response.css(".amenity:contains('Terrace')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False  
        
        suites = response.css(".suites .suite")
        for index, suite in enumerate(suites):
            
            rent = suite.css(".suite-rate .value::text").get()
            if rent:
                rent = rent.split("$")[1].split(".")[0]
            else:
                continue
            
            bathroom_count = suite.css(".suite-bath .value::text").get()
            if bathroom_count:
                bathroom_count = math.floor(float(bathroom_count))
                if bathroom_count == 0:
                    bathroom_count = 1
            else:
                bathroom_count = 1
            
            room_count = suite.css(".suite-type::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
                if room_count == 0:
                    room_count = 1
            else:
                room_count = 1
            
            external_id = f"{str(external_id_root)}_{index}"
            
            
            
            if rent: 
                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
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
                item_loader.add_value("available_date", available_date)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)
                item_loader.add_value("pets_allowed", pets_allowed)
                item_loader.add_value("parking", parking)
                item_loader.add_value("elevator", elevator)
                item_loader.add_value("balcony", balcony)
                item_loader.add_value("terrace", terrace)
                item_loader.add_value("washing_machine", washing_machine)
                item_loader.add_value("dishwasher", dishwasher)
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_email", landlord_email)
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("position", position)

                yield item_loader.load_item()
