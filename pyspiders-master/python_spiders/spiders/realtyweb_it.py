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

class RealtywebSpider(scrapy.Spider):
        
    name = 'realtyweb'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.realtyweb.it']
    start_urls = ['https://www.realtyweb.it/ricerca-proposta/?keyword=&location=any&child-location=any&status=affitto&type=appartamento&min-price=any&max-price=any']

    position = 1


    def parse(self, response):
        
        
        cards = response.css(".property-items-container .property-item")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a.more-details::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                

            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
        
            
            
            RealtywebSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagination a.current + a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      





    def parseApartment(self, response):

        square_meters = response.css(".property-meta span:contains('mq')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".property-meta span:contains('Van')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".property-meta span:contains('Bagn')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css(".price span.status-label + span::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
            
        currency = response.css(".price span.status-label + span::text").get()
        if currency:
            currency = currency.split(",")[0]
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".page-title span::text").get()
        if title:
            title = remove_white_spaces(title)
        
        
        address = title
                
                
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'center: ol.proj.transform\(\[(\d*\.?\d*),(\d*\.?\d*)')
            x = pattern.search(script_map)
            latitude = x.groups()[1]
            longitude = x.groups()[0] 
        
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
    
        description = response.css('.content p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.slides li a::attr(href), #property-featured-image a::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.content p a::attr(href)').getall()

        energy_label = response.css(".arrow-bullet-list li a:contains('Energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split(": ")[1]

        
        floor = response.css(".property-meta span:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor)
            floor = extract_number_only(floor)    

        elevator = response.css(".arrow-bullet-list li a:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False
    
            
        balcony = response.css(".arrow-bullet-list li a:contains('Balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False   
        
        furnished = response.css(".arrow-bullet-list li a:contains('Arredato')::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False   
        
        landlord_email = "info@realtyweb.it"
        landlord_phone = "+390812303236"
        landlord_name = "Realty servizi immobiliari"
        

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
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
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
