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

class RentitfurnishedSpider(scrapy.Spider):
        
    name = 'rentitfurnished_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['rentitfurnished.com']
    
    position = 1

    def start_requests(self):
        start_urls = [
            {'url': 'https://rentitfurnished.com/vancouver/listings/?&fwp_paged=1',
            'city': 'vancouver',
            'landlord_name':'Rent It Furnished',
            'landlord_email':'vancouver@rentitfurnished.com',
            'landlord_phone':'6046283457',
            },
            {'url': 'https://rentitfurnished.com/toronto/listings/?&fwp_paged=1',
            'city': 'toronto',
            'landlord_name':'Rent It Furnished',
            'landlord_email':'toronto@rentitfurnished.com',
            'landlord_phone':'4378363530',
            },
            {'url': 'https://rentitfurnished.com/montreal/listings/?&fwp_paged=1',
            'city': 'montreal',
            'landlord_name':'Rent It Furnished',
            'landlord_email':'montreal@rentitfurnished.com',
            'landlord_phone':'5149779910',
            },
            {'url': 'https://rentitfurnished.com/ottawa/listings/?&fwp_paged=1',
            'city': 'ottawa',
            'landlord_name':'Rent It Furnished',
            'landlord_email':'ottawa@rentitfurnished.com',
            'landlord_phone':'4378363530',
            },
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta = url)


    def parse(self, response):

            
        cards = response.css(".listing")

        for index, card in enumerate(cards):
            
            if card.css(".listing-rented h4::text").get() in ["Rented", "All Inclusive"]:
                continue

            position = self.position
            
            card_url = card.css(".listing-link a::attr(href)").get()

                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }
            
            
            
            RentitfurnishedSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
            
        if len(cards) > 0:
            prev_page = int(parse_qs(response.url)['fwp_paged'][0])
            next_page = int(parse_qs(response.url)['fwp_paged'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&fwp_paged={prev_page}",f"&fwp_paged={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl and nextPageUrl != response.url:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True, meta = response.meta)


    def parseApartment(self, response):
    
        rent = response.css(".price::text").get()
        if rent == "Rented":
            return
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = "CAD"
        
        
        property_type = "apartment"
            
        external_id = response.css("#property-features div.col-sm-2:contains('Property ID')").get()
        if external_id:
            external_id = remove_tags(external_id)
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id).replace(".","")

        square_meters = response.css("#property-intro h3.sticky-title-text").get()
        if square_meters:
            square_meters = remove_tags(square_meters)
            square_meters = remove_white_spaces(square_meters)
            pattern = re.compile(r'(\d+)? sq. ft.', re.IGNORECASE)
            data_from_regex = pattern.search(square_meters)
            if data_from_regex:
                square_meters = data_from_regex.group(1)
                square_meters = sq_feet_to_meters(square_meters)
            else:
                square_meters = None
        
        room_count = response.css("#property-intro h3.sticky-title-text").get()
        if room_count:
            room_count = remove_tags(room_count)
            room_count = remove_white_spaces(room_count)
            pattern = re.compile(r'(\d+)?\+? Bed', re.IGNORECASE)
            data_from_regex = pattern.search(room_count)
            if data_from_regex:
                room_count = data_from_regex.group(1)
            else:
                room_count = None

        bathroom_count = response.css("#property-intro h3.sticky-title-text").get()
        if bathroom_count:
            bathroom_count = remove_tags(bathroom_count)
            bathroom_count = remove_white_spaces(bathroom_count)
            pattern = re.compile(r'(\d+)?\+? Bath', re.IGNORECASE)
            data_from_regex = pattern.search(bathroom_count)
            if data_from_regex:
                bathroom_count = data_from_regex.group(1)
            else:
                bathroom_count = None        

        available_date = response.css("#property-account p.available::text").get()
        if available_date:
            available_date = remove_white_spaces(available_date).split("Available: ")[-1]

        title = response.css("head title::text").get()
        if title:
            title = remove_tags(title)   
            title = remove_white_spaces(title)   
            
        # address = response.css("#property-account p.available ~ p").get()
        address = response.css("#property-account p.available ~ p *::text").getall()
        if address:
            # address = remove_tags(address)   
            address = " ".join(address)   
            address = remove_white_spaces(address)   
        
        city = response.meta["city"]
        
        zipcode = None
        latitude = response.css("#property-map::attr('data-lat')").get()
        longitude = response.css("#property-map::attr('data-lng')").get()
        
        try: 
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=") 
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal'] 
        except Exception as err:
            zipcode = None   
         
    
        # description = response.css("section#property-description").get()
        description = response.css("section#property-description *::text").getall()
        if description:
            # description = remove_tags(description)
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None
        
        
        images = response.css('section#property-gallery a::attr(href)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        

        pets_allowed = response.css("#property-features div.col-sm-2:contains('Pet-Friendly')").get()
        if pets_allowed:
            pets_allowed = remove_tags(pets_allowed)
            pets_allowed = remove_white_spaces(pets_allowed)
            if pets_allowed:
                pets_allowed = True
            else:
                pets_allowed = False      
        else:
            pets_allowed = False   
        
        elevator = response.css("#property-building li:contains('Elevator')").get()
        if elevator:
            elevator = True
        else:
            elevator = False 
        
        balcony = response.css("#property-features li:contains('Balcony')").get()
        if balcony:
            balcony = True
        else:
            balcony = False 
         
            
        swimming_pool = response.css("#property-building li:contains('Pool')").get()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False  

        parking = response.css("#property-features div.col-sm-2:contains('Parking')").get()
        if parking:
            parking = remove_tags(parking)
            parking = remove_white_spaces(parking)
            if parking:
                parking = True
            else:
                parking = False      
        else:
            parking = False  
           
          
        furnished = response.css("#property-intro h3.sticky-title-text").get()
        if furnished:
            furnished = remove_tags(furnished)
            furnished = remove_white_spaces(furnished)
            if "Unfurnished" in furnished:
                furnished = False
            elif "Furnished" in furnished:
                furnished = True
            else:
                furnished = False
            
            
        

        washing_machine = response.css("#property-features li:contains('Laundry')").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False 
        

        try:
            landlord_link = response.css("#property-agent h2 a::attr(href)").get()
            if landlord_link:
                response_landlord_data = requests.get(landlord_link)
                landlord_data = Selector(text=response_landlord_data.text)
            
                landlord_name = landlord_data.css("#intro h1::text").get()
                if landlord_name:
                    landlord_name = remove_white_spaces(landlord_name)
                    
                landlord_phone = landlord_data.css("#intro .contact-details span:nth-of-type(2)::text").get()
                if landlord_phone:
                    landlord_phone = remove_white_spaces(landlord_phone)
                    landlord_phone = landlord_phone.split(": ")[-1].replace("-","").replace(" ","")
                
                landlord_email = landlord_data.css("#intro .contact-details span:nth-of-type(3) a::text").get()
                if landlord_email:
                    landlord_email = remove_white_spaces(landlord_email)       
            else:
                landlord_phone = response.meta["landlord_phone"]
                landlord_email = response.meta["landlord_email"]
                landlord_name = response.meta["landlord_name"]
        except Exception as err:
            landlord_phone = response.meta["landlord_phone"]
            landlord_email = response.meta["landlord_email"]
            landlord_name = response.meta["landlord_name"] 
        
        

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
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
