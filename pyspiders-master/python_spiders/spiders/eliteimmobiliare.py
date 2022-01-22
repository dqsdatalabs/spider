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

class EliteimmobiliareSpider(scrapy.Spider):
        
    name = 'eliteimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.eliteimmobiliare.net']
    start_urls = ['https://www.eliteimmobiliare.net/cerca/?con=1']

    position = 1


    def parse(self, response):
        
        
        cards = response.css(".filter-menu + .row .col-md-3")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                
            city = "Milano"
            
            
            external_id = card.css(".price + span::text").get()
            if external_id:
                external_id = external_id.split(" | ")[1]
        
        
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "city": city,
                "external_id": external_id
            }
            
            
            
            EliteimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        






    def parseApartment(self, response):


        square_meters = response.css("table.table tr td:contains('mq')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("table.table tr td:contains('Local') + td::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css("table.table tr td:contains('Bagn') + td::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css("table.table tr td:contains('Prezzo') + td::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
            
        currency = response.css("table.table tr td:contains('Prezzo') + td::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".singoloIMM h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        address = response.css("table.table tr td:contains('Indirizzo') + td::text").get()
        if address:
            address = remove_white_spaces(address)
                        
        script_map = response.css(".toBigGmap::attr(data-coords)").get()
        if script_map:
            script_map = script_map.split(":")[0].split(",")
            latitude = script_map[0]
            longitude = script_map[1] 
        
    
        description = response.css('.singoloIMM h2 ~ p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('#imageGallery li img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        

        energy_label = response.css("table.table tr td:contains('energetica') + td::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css("table.table tr td:contains('Spese') + td::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        floor = response.css("table.table tr td:contains('Piano') + td::text").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css("table.table tr td:contains('Ascensore') + td::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "no":
                elevator = False
            elif elevator == "si":
                elevator = True
            else:
                elevator = False
        
        furnished = response.css("table.table tr td:contains('Arredamento') + td::text").get()
        if furnished:
            furnished = remove_white_spaces(furnished).lower()
            if furnished == "arredato":
                    furnished = True
            elif furnished == "non arredato":
                furnished = False
            else:
                furnished = False  
             
            
        balcony = response.css("table.table tr td:contains('Balcone') + td::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False   

        terrace = response.css("table.table tr td:contains('Terrazzo') + td::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False    
        
        
        landlord_email = "info@elite.sm"
        landlord_phone = "+390286982980"
        landlord_name = "Elite Immobiliare Srl"

        

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
