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

class AnamuSpider(scrapy.Spider):
        
    name = 'anamu'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.anamu.it']
    start_urls = [
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=appartamento&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&SearchMode=1&PageSize=15&PageNbr=1',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=villa-casa-singola&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=appartamento-duplex&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=appartamento-3c&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=attico&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=mini-appartamento&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=porzione-casa&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        'https://resources.anamu.it/api/Houses/Houses_Search?HouseId=&Prov=&City=&Type=bivilla&Order=upd_desc&Mq_From=&Mq_To=&Price_From=&Price_To=&ContractType=affitto&PageNbr=&SearchMode=1&PageSize=15',
        ]

    position = 1
    
    unit_type = {
        "Appartamento":"apartment",
        "Appartamento 3c":"apartment",
        "Appartamento Duplex":"apartment",
        "Mini Appartamento":"apartment",
        "Attico":"apartment",
        "Monolocale":"apartment",
        "Porzione Casa":"house",
        "Bivilla":"house",
        "Villa/Casa Singola":"house",
    }
    def parse(self, response):
        
        jsonResponse = response.json()
        
        cards = jsonResponse['Houses']
        
        for index, card in enumerate(cards):

            position = self.position
            
            property_type = self.unit_type[card['HOUSE_TYPE']]

            card_url = response.url
                  
            external_id = card['HouseId']
            
            square_meters =  card['Mq']
            
            room_count =  card['N_LOCALI']
            if room_count == 0 :
                room_count = 1
       

            rent = card['PREZZO']
            if rent:
                rent = remove_white_spaces(rent)
                rent = extract_number_only(rent).replace(".","")
            else:
                rent = None
                
            currency = card['PREZZO']
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR"
            
            Prov = card['Prov']
            city = card['CITY_MAIN']
            CITY_SUB = card['CITY_SUB']
            address =f"{CITY_SUB} - {city} - {Prov}"

            description = card['HOUSE_DESC']
            
            images = []
            if card['Photos_URL']:
                for img in card['Photos_URL']:
                    images.append(img)
            external_images_count = len(images)
            
 
            parking = card['N_AUTO']
            if parking != 0:
                parking = True
            else:
                parking = False  
            
            
            landlord_email = "info@anamu.it"
            landlord_phone = "0415413136"
            landlord_name = "ANAMU 'SRL"

            
            external_link = f"https://www.anamu.it/cerchi-casa/?contratto=affitto&riferimento={external_id}"
            
            title = f"House Id {external_id} - {address.replace('-','')} - {square_meters}mq"
            
            if rent and external_images_count != 0: 
                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", external_link)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("external_id", external_id)
                item_loader.add_value("title", title)
                item_loader.add_value("description", description)
                item_loader.add_value("city", city)
                item_loader.add_value("address", address)
                item_loader.add_value("property_type", property_type)
                item_loader.add_value("square_meters", square_meters)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", external_images_count)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)
                item_loader.add_value("parking", parking)
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_email", landlord_email)
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("position", position)
                
                AnamuSpider.position += 1
                
                yield item_loader.load_item()
            

            
        if len(jsonResponse['Houses']) > 0:
            
            prev_page = int(parse_qs(response.url)['PageNbr'][0]) if "PageNbr" in parse_qs(response.url) else None
            
            next_page = int(parse_qs(response.url)['PageNbr'][0]) + 1 if "PageNbr" in parse_qs(response.url) else None
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&PageNbr={prev_page}",f"&PageNbr={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl != response.url:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)







