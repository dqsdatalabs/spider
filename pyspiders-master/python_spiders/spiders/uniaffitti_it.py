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

class UniaffittiSpider(scrapy.Spider):
        
    name = 'uniaffitti'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.uniaffitti.it']
    start_urls = ['https://www.uniaffitti.it/']
    post_url = "https://www.uniaffitti.it/ajax/user"
    start_urls_data = [
        'https://www.uniaffitti.it/listing.php?C=RM&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=SI&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=FI&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=MI&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=PI&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=BO&T=1|2|3|4',
        'https://www.uniaffitti.it/listing.php?C=TO&T=1|2|3|4',
        ]

    position = 1

    def parse(self, response):
        
        X_CSRF_TOKEN = response.css("meta[name='csrf-token']::attr(content)").get()
        
        body_data = {
            "email":"ahmed@rentola.com",
            "password":"gg_EZ",
            "remember":"false",
            "action":"USER-LOGIN"
        }

        yield Request(
            url = self.post_url,
            method = 'post', 
            body = json.dumps(body_data), 
            headers={"Content-Type": "application/json; charset=UTF-8","Accept": "*/*", "Accept-Encoding": "gzip, deflate, br", "X-CSRF-TOKEN": X_CSRF_TOKEN},
            callback = self.parse2,
            dont_filter = True
        )
        

    def parse2(self, response):
        for url in self.start_urls_data:
            yield Request(url = url, callback = self.parse3, dont_filter = True )

    def parse3(self, response):

            
        cards = response.css(".listing__card_offerta")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".listing__card_offerta__info_prezzo a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
   
            
            
            UniaffittiSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination li.current + li a::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse3, dont_filter = True)






    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.url
        if external_id:
            pattern = re.compile(r'-(\d*\.?\d*).html')
            x = pattern.search(external_id)
            external_id = x.groups()[0]
            
        
        
        room_count = response.css(".pull-left:contains('Camere') + .pull-right strong::text").get()
        if room_count == "-":
            room_count = response.css(".pull-left:contains('Posti letto') + .pull-right strong::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
        elif room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".pull-left:contains('Bagni') + .pull-right strong::text").get()
        if bathroom_count == "-":
            bathroom_count = 1
        elif bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
        else:
            bathroom_count = 1         

        rent = response.css(".label:contains('Prezzo') + .value::text, .label:contains('Prezzo') + .value .euro::text").getall()
        rent = " ".join(rent)
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".label:contains('Prezzo') + .value::text, .label:contains('Prezzo') + .value .euro::text").getall()
        currency = " ".join(currency)        
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        


        title = response.css(".annuncio__single-title h1::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address = response.css(".annuncio__single-title h1 + p::text, .annuncio__single-title h1 + p a::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        script_map = response.css("script[type='application/ld+json']::text").getall()
        script_map = " ".join(script_map)
        script_map = remove_white_spaces(script_map)
        if script_map:
            pattern = re.compile(r'"latitude": "(\d*\.?\d*)", "longitude": "(\d*\.?\d*)"')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
            
        if latitude != "0":
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
            
            
        
    
        description = response.css("#Descrizione p::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.single_slide img::attr(data-lazy)').getall()
        external_images_count = len(images)
        
 
        energy_label = response.css(".pull-left:contains('Classe Energetica') + .pull-right strong::text").get()
        if energy_label == "-" or energy_label == "":
            energy_label = None
        elif energy_label:
            energy_label = remove_white_spaces(energy_label)
        else:
            energy_label = None
        
        
        deposit = response.css(".pull-left:contains('Caparra') + .pull-right strong::text").getall()
        deposit = " ".join(deposit)
        deposit = remove_white_spaces(deposit)
        if deposit == "-" or deposit == "":
            deposit = None
        elif deposit:
            deposit = extract_number_only(deposit).replace(".","")
        else:
            deposit = None
            
        available_date = response.css(".pull-left:contains('Disponibile dal') + .pull-right strong::text").get()
        if available_date == "-" or available_date == "":
            available_date = None
        if available_date:
            available_date = remove_white_spaces(available_date)
        else:
            available_date = None

        elevator = response.css("p.dett-ASCENS::attr(class)").get()
        if "dett-disabled" in elevator:
            elevator = False
        elif "dett-disabled" not in elevator:
            elevator = True
        else:
            elevator = False  
            
        
        
        washing_machine = response.css("p.dett-LAVAT::attr(class)").get()
        if "dett-disabled" in washing_machine:
            washing_machine = False
        elif "dett-disabled" not in washing_machine:
            washing_machine = True
        else:
            washing_machine = False  

        
        
        landlord_phone = response.css(".cont_telefono::text").getall()
        landlord_phone = " ".join(landlord_phone)        
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)
        
        landlord_email = response.css(".cont_email::text").getall()
        landlord_email = " ".join(landlord_email)        
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)

        landlord_name = response.css(".nome::text").getall()
        landlord_name = " ".join(landlord_name)        
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)

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
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("deposit", deposit)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
