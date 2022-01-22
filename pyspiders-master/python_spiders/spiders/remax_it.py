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

class RemaxSpider(scrapy.Spider):
        
    name = 'remax'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.remax.it']
    start_urls = ['https://cms.remax.it/api/v1/units?announce=5c5abf0647c29f5b05619b51&unit_type=1&energy_class[]=1&energy_class[]=8&order=sell_price-desc&page=1']

    position = 1

    def parse(self, response):
        
        jsonResponse = response.json()
        
        
        
        if len(jsonResponse['units']['features']) > 0:
            for index, card in enumerate(jsonResponse['units']['features']):

                position = self.position
                
                property_type = "apartment"
                
                external_id = card['properties']['code']
                
                external_link = "https://www.remax.it/en/find/unit/" + card['properties']['slug']
                
                city = f"{card['properties']['municipality']} - {card['properties']['district']}"
                
                longitude = str(card['properties']['location'][0])
                latitude = str(card['properties']['location'][1])
                
                square_meters = card['properties']['square_meters']
                if square_meters:
                    square_meters = str(square_meters).split(".")[0]
                
                room_count = card['properties']['rooms']
                if int(room_count) == 0:
                    room_count = card['properties']['locals']['bedrooms']
                
                bathroom_count = card['properties']['locals']['bathrooms']
                
                
                title = card['properties']['slug']
                
                elevator = card['properties']['services']['elevator']
                terrace = card['properties']['services']['terrace']
                parking = card['properties']['services']['parking']
                elevator = card['properties']['services']['elevator']
                
                rent = card['properties']['sell_price']
                if rent:
                    rent = str(rent).split(".")[0]
                
                currency = "EUR"
                
                
                agentUrl= f"https://www.remax.it/en/find/agents-agencies/agent/{card['properties']['agent']['slug']}"
                landlord_name = card['properties']['agent']['full_name']
                
                responseLandlord = requests.get(agentUrl)
                landlord = Selector(text=responseLandlord.text)
                
                landlord_phone = landlord.css('a.social-list__item::attr(href)').get()
                if landlord_phone:
                    landlord_phone = landlord_phone.split("/")[-1]
                                    
                
                images = []
                for index, img in enumerate(card['properties']['images']):
                    images.append(img['images']['desktop'])
                external_images_count = len(images)
                
                
                dataUsage = {
                    "position": position,
                    "property_type": property_type,
                    "external_id": external_id,
                    "external_link": external_link,
                    "city": city,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "title": title,
                    "elevator": elevator,
                    "terrace": terrace,
                    "parking": parking,
                    "rent": rent,
                    "currency": currency,
                    "agentUrl": agentUrl,
                    "landlord_name": landlord_name,
                    "landlord_phone": landlord_phone,
                    "images": images,
                    "external_images_count": external_images_count,
                    
                }
                
                
                RemaxSpider.position += 1
                yield Request(external_link, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
      
        if len(jsonResponse['units']['features']) > 0:
            
            
            prev_page = int(parse_qs(response.url)['page'][0])
            next_page = int(parse_qs(response.url)['page'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&page={prev_page}",f"&page={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)
        else:
            pass



    def parseApartment(self, response):     

        address = response.css(".unit-overview__location::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
                        
        
    
        description = response.css('.detail-section__content div.text div::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        

        energy_label = response.css(".energy-class__big::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
            
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={response.meta['longitude']},{response.meta['latitude']}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        
        if int(response.meta['rent']) > 0:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta['external_id'])
            item_loader.add_value("title", response.meta['title'])
            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", response.meta['square_meters'])
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
            item_loader.add_value("images", response.meta['images'])
            item_loader.add_value("external_images_count", response.meta['external_images_count'])
            item_loader.add_value("rent", int(response.meta['rent']))
            item_loader.add_value("currency", response.meta['currency'])
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("parking",response.meta['parking'])
            item_loader.add_value("elevator", response.meta['elevator'])
            item_loader.add_value("terrace", response.meta['terrace'])
            item_loader.add_value("landlord_name", response.meta['landlord_name'])
            item_loader.add_value("landlord_phone", response.meta['landlord_phone'])
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
