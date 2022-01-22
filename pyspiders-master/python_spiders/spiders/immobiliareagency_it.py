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

class ImmobiliareagencySpider(scrapy.Spider):
        
    name = 'immobiliareagency'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliareagency.it']
    start_urls = ['https://www.immobiliareagency.it/cerco.php?contr=2&cat=0&type=Appartamento&city=&pricefm=&priceto=&gid=']

    position = 1

    def parse(self, response):
        
        
        
        cards = response.css("#results > div")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css(".permalink a::attr(href)").get()
                
            external_id = card.css(".lssum1-cod::text").get()
            if external_id:
                external_id = remove_white_spaces(external_id).split(" ")[1]
            
            info_text = card.css(".lssum2.lssum2-v2::text").getall()            
            info_text = " ".join(info_text)
            if info_text:
                info_text = remove_white_spaces(info_text)
                info_text = info_text.split(" ")
            
            square_meters = info_text[0]
            room_count =  info_text[3]
            bathroom_count =  info_text[-1]        

            rent = card.css(".lssum2-pz::text").get()
            if rent:
                rent = rent.split(",")[0]
                rent = extract_number_only(rent).replace(".","")
                
            currency = card.css(".lssum2-pz::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR"
            
            title = card_url.split("/")[-1].split(".html")[0]
            

            address = card.css(".lssum1.lssum1-v2::text").getall()            
            address = " ".join(address)
            if address:
                address = remove_white_spaces(address)
            
                            
            city = card.css(".lssum1.lssum1-v2::text").getall()            
            city = " ".join(city)
            if city:
                city = remove_white_spaces(city)
                        

            latitude = card.css(".lsmapbt.btex1::attr(data-coord)").get().split(",")[0]
            longitude = card.css(".lsmapbt.btex1::attr(data-coord)").get().split(",")[1]
            
        
            description = card.css('.dtldesc::text').getall()
            description = " ".join(description)
            description = remove_white_spaces(description)
            
            images = card.css('.tnwrap img::attr(data-src)').getall()
            external_images_count = len(images)
 

            energy_label = card.css(".dtdv:contains('energ')::text").get()
            if energy_label:
                energy_label = remove_white_spaces(energy_label).split(": ")[1]
            
            utilities = card.css(".dtdl:contains('Spese ') + .dtdv::text").get()
            if utilities:
                utilities = remove_white_spaces(utilities)
                utilities = extract_number_only(utilities).split(".")[0]
            
            floor = card.css(".dtdl:contains('Piano') + .dtdv::text").get()   

            elevator = card.css(".dtdl:contains('Ascensore') + .dtdv::text").get()
            if elevator:
                elevator = remove_white_spaces(elevator)
                if elevator == "Sì":
                        elevator = True
                elif elevator == "no":
                    elevator = False
                else:
                    elevator = False 
            
            furnished = card.css("div.dtdl:contains('Arredato') + .dtdv::text").get()
            if furnished:
                furnished = remove_white_spaces(furnished)
                if furnished == "Sì":
                        furnished = True
                elif furnished == "no":
                    furnished = False
                else:
                    furnished = False  
                
                
            balcony = card.css(".dtdl:Contains('Balcon') + .dtdv::text").get()
            if balcony:
                balcony = True
            else:
                balcony = False   

   
            
            landlord_email = "info@immobiliareagency.it"
            landlord_phone = "0817381452"
            landlord_name = "Immobiliare Agency"
                    

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", card_url)
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
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            ImmobiliareagencySpider.position += 1
            yield item_loader.load_item()
            
    