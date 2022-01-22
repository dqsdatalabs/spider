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

class AnsellpropertiesSpider(scrapy.Spider):
        
    name = 'ansellproperties_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['ansellproperties.ca']
    start_urls = ['https://www.ansellproperties.ca/rental/']
    
    position = 1


    def parse(self, response):


            
        cards = response.css(".project-card")

        for index, card in enumerate(cards):
            
            if card.css(".page-price::text").get() in ["Rented", "All Inclusive"]:
                continue

            position = self.position
            
            card_url = card.css(".property-address a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            AnsellpropertiesSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".alignright a::attr(href)").get()
        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "house"
            
        external_id = response.css("#epl-default-map::attr(data-id)").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)  
            

        room_count = response.css(".icon.beds .icon-value::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".icon.bath .icon-value::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)        

        
        rent = response.css(".page-price::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = "CAD"
            

        title = response.css("h1.entry-title").get()
        if title:
            title = remove_tags(title)   
            title = remove_white_spaces(title)   
            

        address = f"{title}, Canada"

        latitude = response.css("#epl-default-map::attr(data-cord)").get().split(",")[0]
        longitude = response.css("#epl-default-map::attr(data-cord)").get().split(",")[1].strip()
        
        try:      
            if latitude and latitude != "0":
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
        except Exception as err:
            pass 
            
    
        description = response.css(".epl-tab-content.epl-tab-content-additional + div.tab-content").get()
        description = remove_tags(description)
        description = remove_white_spaces(description)
        
        
        images = response.css('#epl-slider-slides div > img:first-child::attr(src2)').getall()
        images = [re.sub(r'(-\d*x\d*_c_epl_slider)|(-\d*x\d*)', "", img) for img in images]
        external_images_count = len(images)
        
        
        available_date = response.css(".date-available::text").get()
        if available_date:
            available_date = remove_white_spaces(available_date)

        pets_allowed = response.css(".pet_friendly::text").get()
        if pets_allowed:
            pets_allowed = True
        else:
            pets_allowed = False   

        parking = response.css(".parking").get()
        if parking:
            parking = True
        else:
            parking = False  
            
        
        washing_machine = response.css(".washer-dryer").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False 
        
        dishwasher = response.css(".dishwasher").get()
        if dishwasher:
            dishwasher = True
        else:
            dishwasher = False 
        
        
        
        landlord_phone = "(902) 999-4463"
        landlord_email = "info@ansellproperties.ca"
        landlord_name = "Ansell Properties"


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
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("parking", parking)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
