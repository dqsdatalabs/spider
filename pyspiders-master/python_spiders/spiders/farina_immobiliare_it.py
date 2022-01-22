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

class Farina_immobiliareSpider(scrapy.Spider):
        
    name = 'farina_immobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.farina-immobiliare.it']
    start_urls = ['https://www.farina-immobiliare.it/IT/5-Residenziali/20-Affitto/-/-/-/pagina-1.html?cerca=top&home=true']

    position = 1
    
    def parse(self, response):

        cards = response.css(".span8 .box-container")

        for index, card in enumerate(cards):

            position = self.position
            
            
            card_url = card.css("a.span4::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            Farina_immobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css(".pagination  li.active + li a::attr(href)").get()
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)






    def parseApartment(self, response):

        property_type = response.css(".info-label span:contains('Tipologia') + span::text").get()
        if property_type:
            property_type = remove_white_spaces(property_type).lower()
            property_type = property_type_lookup[property_type]
            
        external_id = response.css(".info-label span:contains('Rif') + span::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css(".info-label span:contains('Mq') + span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".info-label span:contains('Locali') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".info-label span:contains('Bagni') + span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".info-label span:contains('Prezzo') + span::text").get()
        if rent:
            rent = rent.replace(" ","")
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".info-label span:contains('Prezzo') + span::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h1.prop-title::text").get()
        if title:
            title = remove_white_spaces(title)
         
        Comune = response.css(".info-label span:contains('Comune') + span::text").get()
        if Comune:
            Comune = remove_white_spaces(Comune)     
            
        Zona = response.css(".info-label span:contains('Zona') + span::text").get()
        if Zona:
            Zona = remove_white_spaces(Zona)     
            
        Indirizzo = response.css(".info-label span:contains('Indirizzo') + span::text").get()
        if Indirizzo:
            Indirizzo = remove_white_spaces(Indirizzo)
            
        city = Comune
        address = f"{Indirizzo} - {Zona} - {Comune}"   
            
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'immobile_locations\[0\] = new Array\("<strong>\w*</strong>",(\d*\.?\d*),(\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
    
        description = response.css("h3:contains('Descrizione') + p::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.property-slide img::attr(src)').getall()
        external_images_count = len(images)
        
 
        energy_label = response.css(".info-label span:contains('energetica') + span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css(".info-label span:contains('Spese ') + span::text").get()
        if utilities:
            utilities = utilities.replace(" ","")
            utilities = extract_number_only(utilities).replace(".","")
        
        furnished = response.css(".info-label span:contains('Arredamento') + span::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False  
    
        
        parking = response.css(".info-label span:contains('Garage:') + span::text").get()
        if parking:
            parking = True
        else:
            parking = False  
        
        landlord_email = "info@farina-immobiliare.it"
        landlord_phone = "+393911593735"
        landlord_name = "Immobiliare Farina- Real Estate Agency"


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
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
