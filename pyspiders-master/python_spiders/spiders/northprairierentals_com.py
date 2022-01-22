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

class NorthprairierentalsSpider(scrapy.Spider):
        
    name = 'northprairierentals_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['northprairierentals.com']
    start_urls = ['https://northprairierentals.com/rental-properties/']
    
    position = 1

    def parse(self, response):

        
        cards = response.css("h2.section-title + div#property-items .property-item")

        for index, card in enumerate(cards):
            
            if card.css(".listing-rented h4::text").get() in ["Rented", "All Inclusive"]:
                continue

            position = self.position
            
            card_url = card.css("a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            NorthprairierentalsSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = None
        nextPageUrl = response.css("h2.section-title + div#property-items  #pagination a.next::attr(href)").get()

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)

    def parseApartment(self, response):
            
        rent = response.css(".property-header .meta::text").get()
        if rent:
            rent = remove_tags(rent)
            rent = remove_white_spaces(rent)
            pattern = re.compile(r'from \$(\d+,?\d+)? rent', re.IGNORECASE)
            data_from_regex = pattern.search(rent)
            if data_from_regex:
                rent = data_from_regex.group(1)
            else:
                rent = None

        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = "CAD"
        
        property_type = "house"
            
        external_id = response.css("link[rel=shortlink]::attr(href)").get()
        if external_id:
            external_id = remove_tags(external_id)
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id).replace(".","")



        # description = response.css("#main-content #property-content").get()
        description = response.css("#main-content #property-content *::text").getall()
        if description:
            # description = remove_tags(description)
            description = " ".join(description)
            description = remove_white_spaces(description)


        square_meters = response.css(".meta-data[title=Size]::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
            square_meters = sq_feet_to_meters(square_meters)
        else:
            pattern = re.compile(r'(\d+)?\s?sq\.?\s?(?:ft|feet)?\.?', re.IGNORECASE)
            data_from_regex = pattern.search(description)
            if data_from_regex:
                square_meters = data_from_regex.group(1)
                if square_meters: 
                    square_meters = sq_feet_to_meters(square_meters)
            else:
                square_meters = None

        room_count = response.css(".meta-data[title=Bedrooms]::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            pattern = re.compile(r'(\d*\.?\d*?)? bed', re.IGNORECASE)
            data_from_regex = pattern.search(description)
            if data_from_regex:
                room_count = data_from_regex.group(1)
            else:
                room_count = None
        
        bathroom_count = response.css(".meta-data[title=Bathrooms]::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count).split(".")[0]
            bathroom_count = extract_number_only(bathroom_count)
        else:
            pattern = re.compile(r'(\d+)?\.?\d*? baths?', re.IGNORECASE)
            data_from_regex = pattern.search(description)
            if data_from_regex:
                bathroom_count = data_from_regex.group(1)
            else:
                bathroom_count = None


        available_date = response.css('.meta-data[title="Available From"]::text').get()
        if available_date:
            available_date = remove_white_spaces(available_date)


        title = response.css("head title::text").get()
        if title:
            title = remove_tags(title)   
            title = remove_white_spaces(title)   

            
        address = response.css("#location p.text-muted::text").get()
        if address:
            address = remove_white_spaces(address)   
        
        city = None
        zipcode = None
        


        try:        
            if address:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()

                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
                
                longitude = str(longitude)
                latitude = str(latitude)
                
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                    
                responseGeocodeData = responseGeocode.json()
                zipcode = "SK "+ responseGeocodeData['address']['Postal'] +" "+ responseGeocodeData['address']['PostalExt']
                city = responseGeocodeData['address']['City']   
                                 
        except Exception as err:
            pass 
         
        
        images = response.css('#property-carousel div::attr(data-image)').getall()
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]
        external_images_count = len(images)
        
        
        
        
        floor_plan_images = response.css('.panel.panel-default img::attr(src)').getall()
 
 
        pets_allowed = "no pet" in description.lower()
        if pets_allowed:
            pets_allowed = False      
        else:
            pets_allowed = True   
        
        elevator = response.css("#property-features li:contains('Elevator')").get()
        if elevator:
            elevator = True 
        else:
            if "elevator" in description.lower():
                elevator = True 
            else:
                elevator = False
            
        balcony = response.css("#property-features li:contains('Balcony')").get()
        if balcony:
            balcony = True 
        else:
            if "balcony" in description.lower():
                balcony = True 
            else:
                balcony = False
            
        parking = response.css("#property-features li:contains('Parking')").get()
        if parking:
            parking = True 
        else:
            if "parking" in description.lower():
                parking = True 
            else:
                parking = False
        
        washing_machine = response.css("#property-features li:contains('Washer')").get()
        if washing_machine:
            washing_machine = True 
        else:
            if "washer" in description.lower():
                washing_machine = True 
            else:
                washing_machine = False
        
        dishwasher = response.css("#property-features li:contains('Dishwasher')").get()
        if dishwasher:
            dishwasher = True 
        else:
            if "dishwasher" in description.lower():
                dishwasher = True 
            else:
                dishwasher = False
        
        landlord_phone = "3068809372"
        landlord_email = "laura@northprairiehomes.com"
        landlord_name = "Laura Mc Nern"
        

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
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
