# www.studioberetta.net


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

class StudioberettaSpider(scrapy.Spider):
        
    name = 'studioberetta'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.studioberetta.net']
    post_url = 'https://www.studioberetta.net/ita/immobiliLocazione.asp'
    
    position = 1
    formdata = {
        "tipologiaImmobile": "32",
    }
    
    def start_requests(self):
        yield FormRequest(
                        url = self.post_url, 
                        formdata = self.formdata, 
                        callback = self.parse, 
                        dont_filter = True
                        )

    def parse(self, response):
        

        
        cards = response.css(".header3 .media-container-row")
    

        for index, card in enumerate(cards):

     
            
            position = self.position
            property_type = "apartment"
            card_url = card.css(".mbr-figure a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
   
            
            
            StudioberettaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        






    def parseApartment(self, response):


        external_id = response.url.split("id=")[1]
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css("td:contains('M QUADRI') + td::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("td:contains('N. VANI') + td::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1
        
        
        
        
        rent = response.css("td:contains('CANONE') + td::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
                
        currency = response.css("td:contains('CANONE') + td::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR" 
        
            
        city = response.css("h3.mbr-fonts-style strong::text").getall()
        city = " ".join(city)
        if city:
            city = remove_white_spaces(city)
        
        street = response.css("h3 + p.mbr-text::text").getall()
        street = " ".join(street)
        if street:
            street = remove_white_spaces(street)
            
        address = f"{city} - {street}"
        title   = f"{street} - {external_id}"
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'var latlng = new google.maps.LatLng\((\-?\d*\.?\d*), (\-?\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

    
        description = response.css('#content1-a > div > div > div.mbr-text.col-12.col-md-8.mbr-fonts-style.display-7::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.carousel-inner .carousel-item img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
            
        parking = response.css("td:contains('PARCHEGGIO') + td::text").get()
        if parking:
            parking = parking.lower()
            if parking=="si":
                parking = True
            elif parking=="no":
                parking = False
            else:
                parking = False 

        terrace = response.css("td:contains('SPAZI ESTERNI') + td::text").get()
        if terrace:
            terrace = terrace.lower()
            if terrace=="si":
                terrace = True
            elif terrace=="no":
                terrace = False
            else:
                terrace = False    
          
        
        landlord_name = "Studio Immobiliare Beretta"
        landlord_phone = "+39055286996"
        landlord_email = "info@studioberetta.net"
                
        
        if rent:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
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
            item_loader.add_value("parking", parking)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
