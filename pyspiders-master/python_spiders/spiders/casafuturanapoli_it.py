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

class CasafuturanapoliSpider(scrapy.Spider):
        
    name = 'casafuturanapoli'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.casafuturanapoli.it']
    start_urls = ['https://www.casafuturanapoli.it/cerco.php?contr=2&cat=0&type=Appartamento&city=&pricefm=&priceto=&gid=']

    position = 1
    
    
    def parse(self, response):

            
        cards = response.css(".estates")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".permalink a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            CasafuturanapoliSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
 

    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css(".lssum1-cod::text").getall()
        external_id = " ".join(external_id)
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)
            
            
        info = response.css(".lssum2::text").getall()
        info = " ".join(info)
        info = remove_white_spaces(info)
        info = info.split(" ")

        square_meters = info[0]
        
        room_count = info[3]

        bathroom_count = info[-1]        

        rent = response.css(".lssum2-pz::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".lssum2-pz::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("head title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address = response.css(".lssum1::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = address.split(" ")[0]
            

        script_map = response.css(".lsmapbt::attr(data-coord)").get()
        if script_map:
            script_map = script_map.split(",")
            latitude = script_map[0]
            longitude = script_map[1]

        
    
        description = response.css(".dtldesc::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.dtlimg img::attr(src)').getall()
        external_images_count = len(images)
        
 
        energy_label = response.css(".dtdv:contains('energ')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split(": ")[1]
        
        utilities = response.css(".dtdl:contains('Spese cond.:') + .dtdv::text").get()
        if utilities:
            utilities = extract_number_only(utilities).split(".")[0]
        
        floor = response.css(".dtdl:contains('Piano:') + .dtdv::text").get()
        if floor:
            floor = remove_white_spaces(floor)
        

        elevator = response.css(".dtdl:contains('Ascensore') + .dtdv::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  
            
        balcony = response.css(".dtdl:contains('Balconi') + .dtdv::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
        
        landlord_email = "info@casafuturanapoli.it"
        landlord_phone = "0810124949"
        landlord_name = "AG. IMMOBILIARE CASA FUTURA SECONDIGLIANO"


        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
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
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
