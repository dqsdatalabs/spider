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

class ImmobiliareinsiemeSpider(scrapy.Spider):
        
    name = 'immobiliareinsieme'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['immobiliareinsieme.it']
    start_urls = ['https://immobiliareinsieme.it/ricerca-avanzata/?filter_search_action%5B%5D=affitto&advanced_city=&advanced_area=&property_status=&price_low=0&price_max=1500000&riscaldamento=&ricerca-libera=&wpestate_regular_search_nonce=567558051b&_wp_http_referer=%2Fi-nostri-immobili-immobiliareinsieme%2F']
    
    position = 1

    def parse(self, response):


            
        cards = response.css(".listing_wrapper > .property_listing")

        for index, card in enumerate(cards):

            if "Negozio" in card.css("h4 a::text").get():
                continue
            
            position = self.position
            
            card_url = card.css("h4 a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            ImmobiliareinsiemeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination li.roundright a::attr(href)").get()

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("#propertyid_display::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)


        square_meters = response.css(".listing_detail:contains('Dimensioni immobile:')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters).split(".")[0]
            square_meters = extract_number_only(square_meters)
            

        room_count = response.css(".listing_detail:contains('Locali:')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = response.css(".listing_detail:contains('Camere da letto:')::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)

        bathroom_count = response.css(".listing_detail:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        
        rent = response.css(".price_area::text").get()
        if "Trattativa Riservata" not in rent:
            if rent:
                rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".price_area::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h1.entry-title::text").get()
        if title:
            title = remove_white_spaces(title)   
            

        title = response.css("h1.entry-title::text").get()
        if title:
            title = remove_white_spaces(title)   

        address = response.css("span.adres_area::text, span.adres_area a::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)

        latitude = response.css("#googleMap_shortcode::attr(data-cur_lat)").get()
        longitude = response.css("#googleMap_shortcode::attr(data-cur_long)").get()
                    
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
    
        description = response.css(".wpestate_property_description p::text").get()
        description = remove_white_spaces(description)
        
        
        images = response.css('#owl-demo .item::attr(style)').getall()
        images = [img.replace("background-image:url(","").replace(")","") for img in images]
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]
        external_images_count = len(images)

 
 
        energy_label = response.css(".listing_detail:contains('Energy class:')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).upper()
        
        
        floor = response.css(".listing_detail:contains('Piano:')::text").get()
        if floor:
            floor = remove_white_spaces(floor)

        
        elevator = response.css("#accordion_prop_features .listing_detail:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  
            
        balcony = response.css("#accordion_prop_features .listing_detail:contains('Balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
        terrace = response.css("#accordion_prop_features .listing_detail:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False  
            

        parking = response.css("#accordion_prop_features .listing_detail:contains('Posto Auto')::text").get()
        if parking:
            parking = True
        else:
            parking = False  
            
            
        furnished = response.css("#accordion_prop_features .listing_detail:contains('Arredato')::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False  
        
        
        
        landlord_phone = response.css(".agent_unit .agent_detail a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")
        
        landlord_email = response.css(".agent_unit .agent_detail a:contains('@')::text").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
        
        landlord_name = response.css(".agent_unit h4 a::text").get()
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
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
