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
from w3lib.html import remove_tags


class olxSpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'olx_{locale}'
    external_source = f"olx_PySpider_{country}_{locale}"
    allowed_domains = ['olx.pt']

    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.olx.pt/ads/q-arrendar/',
                'property_type': 'apartment',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):
    

        
        cards = response.css(".redesigned tr.wrap")

        for index, card in enumerate(cards):


            position = self.position

            external_id = card.css("table::attr(data-id)").get()
            card_url=None
            
            if external_id:
                try:
                    responseData = requests.get(f"https://www.olx.pt/api/v1/offers/{external_id}/")
                    responseDataJson = responseData.json()["data"]
                except Exception as err:
                    continue
                
                
                card_url = responseDataJson['url']
                title = responseDataJson['title']
                description = responseDataJson['description']
                longitude = responseDataJson['map']['lon']
                latitude = responseDataJson['map']['lat']
                landlord_name = responseDataJson['contact']['name']
                ifPhone = responseDataJson['contact']['phone']
                city = responseDataJson['location']['city']['name']
                region = responseDataJson['location']['region']['name']
                
                images = []
                if responseDataJson['photos']:
                    for img in responseDataJson['photos']:
                        link = img["link"].replace("{width}",str(img["width"])).replace("{height}",str(img["height"]))
                        images.append(link)
                
                
                
                rent = next((x for x in responseDataJson['params'] if x["key"] == "price"), None)
                if rent:
                    rent = rent['value']['value']
                
                
                square_meters = next((x for x in responseDataJson['params'] if x["key"] == "area_m2"), None)
                if square_meters:
                    square_meters = square_meters['value']['label']
                    square_meters = remove_white_spaces(square_meters)
                    square_meters = extract_number_only(square_meters)
                
                room_count = next((x for x in responseDataJson['params'] if x["key"] == "tipologia"), None)
                if room_count:
                    room_count = room_count['value']['label']
                    room_count = remove_white_spaces(room_count)
                    room_count = extract_number_only(room_count)
                
                bathroom_count = next((x for x in responseDataJson['params'] if x["key"] == "casas_de_banho"), None)
                if bathroom_count:
                    bathroom_count = bathroom_count['value']['label']
                    bathroom_count = remove_white_spaces(bathroom_count)
                    bathroom_count = extract_number_only(bathroom_count)
                    
                energy_label = next((x for x in responseDataJson['params'] if x["key"] == "certificado_energetico"), None)
                if energy_label:
                    energy_label = energy_label['value']['label']
                    energy_label = remove_white_spaces(energy_label)
                                    
                furnished = next((x for x in responseDataJson['params'] if x["key"] == "mobilado"), None)
                if furnished:
                    furnished = furnished['value']['label']
                    furnished = True if furnished == "Sim" else False
                
                pets_allowed = next((x for x in responseDataJson['params'] if x["key"] == "animais_de_estimacao"), None)
                if pets_allowed:
                    pets_allowed = pets_allowed['value']['label']
                    pets_allowed = True if pets_allowed == "Sim" else False
                
            else:
                continue

            dataUsage = {
                "position": position,
                "external_id": external_id,
                "card_url": card_url,
                "title": title,
                "description": description,
                "longitude": longitude,
                "latitude": latitude,
                "landlord_name": landlord_name,
                "ifPhone": ifPhone,
                "images": images,
                "rent": rent,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "energy_label": energy_label,
                "furnished": furnished,
                "pets_allowed": pets_allowed,
                "region": region,
                "city": city,
                **response.meta
            }

            olxSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0:       
            nextPageUrl = response.css(".pager .next a::attr(href)").get()
            if nextPageUrl and nextPageUrl != response.url:
                yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta=response.meta)

            
    


    def parseApartment(self, response):
        

        
        rent = response.meta['rent']
        if not rent:
            return


        currency = "EUR"
        property_type = response.meta['property_type']
        position = response.meta['position']
        external_id = response.meta['external_id']
        square_meters = response.meta['square_meters']
        room_count = response.meta['room_count'] 
        bathroom_count = response.meta['bathroom_count']
        description = response.meta['description']
        title = response.meta['title']
        
        address = f"{response.meta['city']}, {response.meta['region']}, portgual"  
        city = response.meta['city']
        zipcode = None
        longitude = (response.meta['longitude'])
        if longitude:
            longitude = str(longitude)
        latitude = response.meta['latitude']
        if latitude:
            latitude = str(latitude)

        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                address = responseGeocodeData['address']['LongLabel']
                city = responseGeocodeData['address']['City']
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            pass

        images = response.meta['images']
        # images = [response.urljoin(img) for img in images]
        external_images_count = len(images)


        energy_label = response.meta['energy_label']
            

            
        furnished = response.meta['furnished']
        
        pets_allowed = response.meta['pets_allowed']

        
        
        landlord_name = response.meta['landlord_name']
        landlord_email = "noreply@olx.pt"

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
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
