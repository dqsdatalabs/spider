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

class ImmobiliareazimutSpider(scrapy.Spider):
        
    name = 'immobiliareazimut'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliareazimut.it']
    start_urls = ['https://www.immobiliareazimut.it/ita/immobili?order_by=&page=&rental=1&company_id=&seo=&luxury=&investment=&categories_id=&rental=1&property_type_id=1&city_id=&size_min=&size_max=&price_min=&price_max=&code=&coords=&coords_center=&coords_zoom=']

    position = 1


    def parse(self, response):


            
        cards = response.css(".card")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".foto::attr(href)").get()

                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
        
            
            
            ImmobiliareazimutSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css("li.pager-pages.current + li a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css(".code::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id).split(". ")[1]

        square_meters = response.css("span:contains('MQ') + b::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("span:contains('Locali') + b::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1
     

        rent = response.css("span:contains('Prezzo') + b::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css("span:contains('Prezzo') + b::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        

        title = response.css("head > title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        
        address = response.css(".location::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = address.split(",")[0]
            

        script_map = response.css("#tab-map script::text").get()
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r'var fenway = new google.maps.LatLng\((\d*\.?\d*),(\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
            
    
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        
        zipcode = responseGeocodeData['address']['Postal']
        
    
        description = response.css(".description::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.slider-for a::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.planimetries_list a::attr(href)').getall()
 
        energy_label = response.css("span:contains('Classe Energ.') + b::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css("span:contains('Spese Annuali') + b::text").get()
        if utilities:
            utilities = int(extract_number_only(utilities).replace(".","")) // 12
        
        floor = response.css("span:contains('Piano') + b::text").get()
        if floor:
            floor = remove_white_spaces(floor)
        

        pets_allowed = response.css("span:contains('Animali Ammessi') + b span::attr(class)").get()
        if pets_allowed:
            if 'presence' in pets_allowed:
                pets_allowed = True
        else:
            pets_allowed = False  
        
        elevator = response.css("span:contains('Ascensore') + b span::attr(class)").get()
        if elevator:
            if 'presence' in elevator:
                elevator = True
        else:
            elevator = False  
            
        balcony = response.css("span:contains('Balcone/i') + b span::attr(class)").get()
        if balcony:
            if 'presence' in balcony:
                balcony = True
        else:
            balcony = False  
        
        terrace = response.css("span:contains('Terrazzo/i') + b span::attr(class)").get()
        if terrace:
            if 'presence' in terrace:
                terrace = True
        else:
            terrace = False  
            
        parking = response.css("span:contains('Posti Auto') + b span::attr(class)").get()
        if parking:
            if 'presence' in parking:
                parking = True
        else:
            parking = False  
            
            
        
        landlord_phone = response.css(".info .email + a::attr(href)").get()      
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1]
            landlord_phone = remove_white_spaces(landlord_phone)
        
        landlord_email = response.css(".info .email::text").getall()
        landlord_email = " ".join(landlord_email)        
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)

        landlord_name = response.css(".info .name::text").getall()
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
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("pets_allowed", pets_allowed)
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
