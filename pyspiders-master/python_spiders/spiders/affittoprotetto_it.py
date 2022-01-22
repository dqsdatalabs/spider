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

class AffittoprotettoSpider(scrapy.Spider):
        
    name = 'affittoprotetto'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.affittoprotetto.it']
    start_urls = ['https://www.affittoprotetto.it/contratto/affitto-immobile/']

    position = 1



    def parse(self, response):

            
        cards = response.css(".item-listing-wrap")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".item-title a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            AffittoprotettoSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css("li.active + li a::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)



    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("strong:contains('Codice') + span::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css("strong:contains('Dimensione') + span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
            

        room_count = response.css("strong:contains('Vani') + span::text, strong:contains('Vano') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = response.css("strong:contains('Camere da Letto') + span::text, strong:contains('Camera da Letto') + span::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)

        bathroom_count = response.css("strong:contains('Bagno') + span::text, strong:contains('Bagni') + span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        price = response.css("strong:contains('Prezzo') + span::text").get()
        if price:
            price = remove_white_spaces(price)        

        if "settimana" in price:
            rent =  price
            if rent:
                rent = int(extract_number_only(rent).replace(".",""))*4
            else:
                rent = None
        else:
            rent =  price
            if rent:
                rent = extract_number_only(rent).replace(".","")
            else:
                rent = None
            
        currency =  price
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        
        
        

        title = response.css(".page-title h1::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        
        address = response.css(".item-address::text").get()
        if address:
            address = remove_white_spaces(address)
            
            

        script_map = response.css("#houzez-single-property-map-js-extra::text").get()
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r'"lat":"(\d*\.?\d*)","lng":"(\d*\.?\d*)",')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
            
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        

    
        description = response.css("div#property-description-wrap .block-content-wrap p::text, div#property-description-wrap .block-content-wrap li::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.lightbox-slider img::attr(src)').getall()
        external_images_count = len(images)
        
 
        energy_label = response.css("strong:contains('Classe Energetica') + span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        
        floor = response.css("strong:contains('Piano') + span::text").get()
        if floor:
            floor = remove_white_spaces(floor)
        
        
        elevator = response.css("#property-features-wrap a:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  

        
        terrace = response.css("#property-features-wrap a:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False  
            
        parking = response.css("#property-features-wrap a:contains('Posto Auto')::text").get()
        if parking:
            parking = True
        else:
            parking = False  
            
            
        furnished = response.css("#property-features-wrap a:contains('Arredato')::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False  
            
        

        
        
        landlord_phone = response.css(".agent-phone a::attr(href)").get()      
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1].replace(".","")
            landlord_phone = remove_white_spaces(landlord_phone)
        
        landlord_email = "info@affittoprotetto.it"
        
        landlord_name = response.css(".agent-name::text").get()
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
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
