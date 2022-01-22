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

class ItalcaseimmobiliareSpider(scrapy.Spider):
        
    name = 'italcaseimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.italcaseimmobiliare.com']
    start_urls = ['https://www.italcaseimmobiliare.com/elenco-immobili?affitto&id_tipo_cerca=2&categoria_cerca=1&cerca=ok']

    position = 1

    def parse(self, response):
        
        
        cards = response.css(".property-block-three")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                
            city = card.css(".lucation strong::text").get()
            if city:
                city = remove_white_spaces(city)
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "city": city
            }
            
            
            
            ItalcaseimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".styled-pagination .next a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      





    def parseApartment(self, response):


        external_id = response.css(".property-detail .row:nth-child(1) div:contains('Codice') strong::text").get()
        if external_id:
            external_id = external_id
        
        square_meters = response.css(".colora_row div:contains('mq') + div::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".colora_row div:contains('Numero locali') + div::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".colora_row div:contains('Bagni') + div::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css(".price::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
            
        currency = response.css(".price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".property-detail h3.title::text").get()
        if title:
            title = remove_white_spaces(title)
        
        street = response.css("span.location::text").getall()
        if street:
            street = " ".join(street)
            street = remove_white_spaces(street)
        
        address = f"{street} - {response.meta['city']}"
                
        script_map = response.css("iframe::attr(src)").get()
        if script_map:
            pattern = re.compile(r'maps.google.com\/maps\?q=(\d*\.?\d*),(\d*\.?\d*)&')
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
        
    
        description = response.css('.property-detail h3 + div.mb-40:first-of-type::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.image-carousel li a::attr(href)').getall()
        external_images_count = len(images)
        

        energy_label = response.css(".colora_row div:contains('Energetica') + div::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css(".colora_row div:contains('Spese') + div::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        floor = response.css(".colora_row div:contains('Piano') + div::text").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css(".colora_row div:contains('Ascensore') + div::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "no":
                elevator = False
            elif elevator == "si":
                elevator = True
            else:
                elevator = False
        
        furnished = response.css(".colora_row div:contains('Arredamento') + div::text").get()
        if furnished:
            furnished = remove_white_spaces(furnished).lower()
            if furnished == "arredato":
                    furnished = True
            elif furnished == "non arredato":
                furnished = False
            else:
                furnished = False  
             
            
        balcony = response.css(".colora_row div:contains('Balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False   

        terrace = response.css(".colora_row div:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False    
        
        parking = response.css(".colora_row div:contains('Posti auto')::text,.colora_row div:contains('Posto auto')::text").get()
        if parking:
            parking = True
        else:
            parking = False  
        
        
        landlord_email = "info@italcaseimmobiliare.com"
        landlord_phone = "+390805580010"
        landlord_name = "ITALCASE IMMOBILIARE"
                

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
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
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
