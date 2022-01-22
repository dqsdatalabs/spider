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

class CasamercatopalermoSpider(scrapy.Spider):

    name = 'casamercatopalermo'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.CasamercatopalermoSpider.it']
    post_url = "https://casamercatopalermo.it/api/AdsSearch/PostMiniFichasAdsMaps"

    position = 1
    
    body_data = {
        "IsAuction": "false",
        "IsRent": "true",
        "IsSale": "false",
        "adOperationId": "2",
        "adTypologyId": "1",
        "currentPage": "0",
    }


    def start_requests(self):
        yield Request(
            url = self.post_url,
            method = 'post', 
            body = json.dumps(self.body_data), 
            headers={"Content-Type": "application/json; charset=UTF-8","Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"},
            callback = self.parse,
            dont_filter = True
        )

    def parse(self, response):


        jsonResponse = response.json()
        
        cards = jsonResponse['ads']

        for index, card in enumerate(cards):

            position = self.position

            property_type = "apartment"

            card_url = card["id"]
            if card_url:
                card_url = f"https://casamercatopalermo.it/ad/{card_url}"

            zipcode = card["property"]["address"]["postalCode"]
            
            
            images = []
            for img in card["multimedias"]["pictures"]:
                images.append(f"https://img3.idealista.it/blur/HOME_WI_1500/0/{img['masterPath']}{img['masterName']}")
            external_images_count = len(images)
            
            
            
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "zipcode": zipcode,
                "images": images,
                "external_images_count": external_images_count,
            }


            CasamercatopalermoSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

 


    def parseApartment(self, response):

        external_id = response.css(".property-ref::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id).split("Ref. ")[1]

        square_meters = response.css(".prices-wrap + ul li:contains('m²')::text").getall()
        square_meters = " ".join(square_meters)
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".prices-wrap + ul li:contains('Loc')::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".prices-wrap + ul li:contains('Bagn')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".price::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("#titulo::text").get()
        if title:
            title = remove_white_spaces(title)
                         
        city = title.split(", ")[1]
        address = title.split("In affitto ")[1]
            
        latitude = response.css('#map::attr(data-ltd)').get().replace(",",".")
        longitude = response.css('#map::attr(data-lng)').get().replace(",",".")
        
    
        description = response.css('.prices-wrap ~ p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

        
        energy_label = response.css("ul li:contains('energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split("energetica ")[1]
        

        
        floor = response.css("ul li:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor).split("Piano ")[1]

        elevator = response.css("ul li:contains('ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False
        
        furnished = response.css("ul li:contains('arredata')::text").get()
        if "non arredata" in furnished:
            furnished = False
        elif "non arredata" not in furnished:
            furnished = True
        else:
            furnished = False  
              
        
        terrace = response.css("ul li:contains('Terrazza')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False      
        
        landlord_email = "immobiliare@casamercatopalermo.it"
        landlord_phone = "+390916111612"
        landlord_name = "Casa Mercato"

        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", response.meta['images'])
            item_loader.add_value("external_images_count", response.meta['external_images_count'])
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
