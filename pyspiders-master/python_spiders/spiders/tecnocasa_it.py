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

class TecnocasaSpider(scrapy.Spider):
        
    name = 'tecnocasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.tecnocasa.it']

    position = 1
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.tecnocasa.it/affitto-appartamenti.html',
            'property_type': 'apartment'},
            {'url': 'https://www.tecnocasa.it/affitto-ville.html',
            'property_type': 'house'},
            {'url': 'https://www.tecnocasa.it/affitto-rustici-casali.html',
            'property_type': 'house'},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse_1, dont_filter=True, meta={'property_type': url.get('property_type')})
    def parse_1(self, response):
        
        links = response.css("ul:not(.with-cities-padding) > li.with-cities a::attr(href)").getall()
        for link in links:
            yield Request(url=link, callback=self.parse_2, dont_filter=True, meta={'property_type': response.meta['property_type']})
    
    def parse_2(self, response):
        links = response.css(".index-box > li.with-cities a::attr(href)").getall()
        for link in links:
            yield Request(url=link, callback=self.parse_3, dont_filter=True, meta={'property_type': response.meta['property_type']})
    def parse_3(self, response):
    

            
        cards = response.css("estate-card")

        for index, card in enumerate(cards):
            
            jsonDataText = card.attrib[':estate']
            jsonData = json.loads(jsonDataText)
            
            
            
            position = self.position
            
            property_type = response.meta['property_type']
            
            card_url = jsonData["detail_url"]

                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            

            
            TecnocasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination a:contains('>')::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse_3, dont_filter = True, meta={'property_type': response.meta['property_type']})






    def parseApartment(self, response):

        jsonDataText = response.css("estate-show").attrib[':estate']
        jsonData = json.loads(jsonDataText)
        

        external_id = jsonData["id"]
        if external_id:
            external_id = str(external_id)
        
        
        square_meters = jsonData["surface"]
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = jsonData["rooms"]
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = jsonData["bathrooms"]
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".col:contains('Prezzo:') + .col strong::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".col:contains('Prezzo:') + .col strong::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("head > title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address = response.css("h2.estate-subtitle::text").get()
        if address:
            address = remove_white_spaces(address)       

        latitude = jsonData["latitude"]
        if latitude:
            latitude = str(latitude)
            
        longitude = jsonData["longitude"]
        if longitude:
            longitude = str(longitude)
  

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']

    
        description = jsonData["description"]
        
        imagesLinks = jsonData["media"]["images"]
        images = []
        for img in imagesLinks:
            images.append(img['url'])
            
        external_images_count = len(images)
        
        floor_plan_images = jsonData["media"]["floor_plans"]
        if floor_plan_images:
            floor_plan_images = [img['url'] for img in floor_plan_images]
 
        energy_label = jsonData["energy_data"]["class"]
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = jsonData["costs"]["expenses"]
        if utilities:
            utilities = int(extract_number_only(utilities).replace(".","")) // 12
        
        floor = jsonData["features"]["floor"]
        if floor:
            floor = remove_white_spaces(floor)

        elevator = jsonData["features"]["elevator"]
        if elevator == 'Sì' and elevator != None:
            elevator = True
        else:
            elevator = False  
            
        balcony = jsonData["features"]["balconies"]
        if balcony != 0 and balcony != None:
            balcony = True
        else:
            balcony = False  
        
        furnished = jsonData["features"]["furnitured"]
        if furnished == "Assente":
            furnished = False
        elif furnished == "Completo":
            furnished = True
        elif furnished == "Parziale":
            furnished = True
        else:
            furnished = False
          
              
        parking = jsonData["features"]["car_places"]
        if parking != 0 and parking != None:
            parking = True
        else:
            parking = False  
        
        terrace = jsonData["features"]["terraces"]
        if terrace != 0 and terrace != None:
            terrace = True
        else:
            terrace = False     
        
        
        
        landlord_email = jsonData["agency"]["email"]
        landlord_phone = jsonData["agency"]["phone"]
        landlord_name = jsonData["agency"]["name"]


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
            item_loader.add_value("property_type", response.meta['property_type'])
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
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
