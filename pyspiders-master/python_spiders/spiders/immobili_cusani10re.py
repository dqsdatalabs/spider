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

class Immobili_cusani10reSpider(scrapy.Spider):
        
    name = 'immobili_cusani10re'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['immobili.cusani10re.com']
    start_urls = ['https://immobili.cusani10re.com/?type=affitto&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=']

    position = 1



    def parse(self, response):
            
        cards = response.css("article.property-listing-simple")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".property-description a.btn-default::attr(href)").get()

                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            Immobili_cusani10reSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination .current + a::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)







    def parseApartment(self, response):

        check_flag_if_ad_rented = response.css(".meta-item-label:contains('Stato') + .meta-item-value::text").get()
        if check_flag_if_ad_rented == "Affittato":
            return

        property_type = "apartment"
            
        external_id = response.css(".meta-item-label:contains('ID Immobile') + .meta-item-value::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css(".meta-item-label:contains('Area') + .meta-item-value::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
            

        room_count = response.css(".meta-item-label:contains('Camere da letto') + .meta-item-value::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".meta-item-label:contains('Bagni') + .meta-item-value::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        
        rent = response.css(".single-property-price.price::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".single-property-price.price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        
        
        

        title = response.css(".single-property-title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        
            
            
        city = response.css(".meta-item-label:contains('Luogo') + .meta-item-value::text").get()
        if city:
            city = remove_white_spaces(city)   
            
        try:
            script_map = response.css("#property-google-map-js-extra::text").get()
            if script_map:
                script_map = remove_white_spaces(script_map)
                pattern = re.compile(r'"lat":"(\d*\.?\d*)","lang":"(\d*\.?\d*)",')
                x = pattern.search(script_map)
                latitude = x.groups()[0]
                longitude = x.groups()[1]
                
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            
            zipcode = responseGeocodeData['address']['Postal']
            address = responseGeocodeData['address']['LongLabel']
        except Exception as err:
            pass
    
        description = response.css(".property-content > *:not(div[role='form'])").getall()
        # if description.index("<p><strong>Per Info:</strong></p>"):
        #     description = description[:description.index("<p><strong>Per Info:</strong></p>")]
        inx = 0
        for index, item in enumerate(description):
            if "per info:" in item.lower():
                inx = index
                break        
        description = description[:inx]    
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        
        
        
        images = response.css('.slides a::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.floor-plan img::attr(src)').getall()
 
 
 
        energy_label = response.css("dt:contains('Classe energetica') + dd::text").get()
        if energy_label:
            if "-" in energy_label:
                energy_label = remove_white_spaces(energy_label).split(" - ")[0]
            else:
                energy_label = remove_white_spaces(energy_label)
        
        
        
        
        utilities = response.css("dt:contains('Spese Condominiali') + dd::text").get()
        if utilities:
            utilities = extract_number_only(utilities).split(".")[0]
        
        
        
        
        floor = response.css("dt:contains('Locali') + dd::text").get()
        if floor:
            floor = remove_white_spaces(floor).split(" - ")[1]
        
 
            
        balcony = response.css(".property-features-list a:contains('Bilocale')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
        terrace = response.css("dt:contains('terrazzo') + dd::text").get()
        if terrace:
            if "si" in terrace:
                terrace = True
        else:
            terrace = False  
            
        parking = "Posto auto".lower() in description.lower()
        if parking:
            parking = True
        else:
            parking = False  

            
            
        furnished = response.css("dt:contains('Arredamento') + dd::text").get()
        if furnished:
            furnished = furnished.lower()
            if furnished == "si":
                furnished = True
            elif furnished == "presente":
                furnished = True
            elif "cucina" in furnished:
                furnished = False
            else:
                furnished = False  
            
        

        
        landlord_phone = "0287085728"
        landlord_email = "info@cusani10re.com"
        landlord_name = "Cusani10 Luxury Real Estate"

 

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
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
