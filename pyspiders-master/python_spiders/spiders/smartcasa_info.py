import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class SmartcasaSpider(scrapy.Spider):
        
    name = 'smartcasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.smartcasa.info']

    position = 1
    
    def start_requests(self):
        start_urls = [
            {'url': 'http://www.smartcasa.info/r/annunci/affitto-appartamento-.html?Codice=&Tipologia%5B%5D=1&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Locali_da=&Locali_a=&Camere_da=&Camere_a=&cf=yes',
            'property_type': 'apartment'},
            {'url': 'http://www.smartcasa.info/r/annunci/affitto-villa-.html?Codice=&Tipologia%5B%5D=9&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Locali_da=&Locali_a=&Camere_da=&Camere_a=&cf=yes',
            'property_type': 'house'},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta={'property_type': url.get('property_type')})

    def parse(self, response):
            
        cards = response.css(".realestate .realestate-lista")

        for index, card in enumerate(cards):

            position = self.position
            property_type = response.meta['property_type']
            card_url = card.css("a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
                "property_type": property_type,
            }
            
            
            SmartcasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            


    def parseApartment(self, response):

            
        external_id = response.css(".tit_sez.codice::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id).split(": ")[1]

        square_meters = response.css(".informazioni .grid-6:contains('mq')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".informazioni .grid-6:contains('Locali')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".informazioni .grid-6:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".tit_sez.prezzo::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".tit_sez.prezzo::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("head title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
    
        description = response.css(".testo p::text, .testo p strong::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        
                
        Regione = response.css(".informazioni .grid-6:contains('Regione')::text").get()
        if Regione:
            Regione = remove_white_spaces( Regione.replace(":","") )
            
        Comune = response.css(".informazioni .grid-6:contains('Provincia')::text").get()
        if Comune:
            Comune = remove_white_spaces( Comune.replace(":","") )
            
        Provincia = response.css(".informazioni .grid-6:contains('Comune')::text").get()
        if Provincia:
            Provincia = remove_white_spaces( Provincia.replace(":","") )
            
        zona = response.css(".informazioni .grid-6:contains('Zona')::text").get()
        if zona:
            zona = remove_white_spaces( zona.replace(":","") )
            
        address = f"{zona} - {Comune} - {Provincia} - {Regione}"
            
        city = Comune
        
        
        script_map = response.css(".feedset-int script::text").getall()
        script_map = " ".join(script_map)
        script_map = remove_white_spaces(script_map)
        if script_map:
            pattern = re.compile(r'var lat = "(\d*\.?\d*)"; var lgt = "(\d*\.?\d*)";')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        
        
        images = response.css('#images li a::attr(href)').getall()
        external_images_count = len(images)

        energy_label = response.css(".classe_energ p:contains('energetica') + div ::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css(".informazioni .grid-6:contains('Spese')::text").get()
        if utilities:
            utilities = extract_number_only(utilities)
        
        floor = response.css(".informazioni .grid-6:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor).replace(": ","")


        elevator = response.css(".informazioni .grid-6:contains('Ascensore')::text").get()
        if "Si" in elevator:
            elevator = True
        else:
            elevator = False  
            
        balcony = response.css(".informazioni .grid-6:contains('Balconi')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
        
        landlord_email = "Immobiliare@smartcasa.info"
        landlord_phone = "0915084949"
        landlord_name = "Smartcasa"


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
            item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()