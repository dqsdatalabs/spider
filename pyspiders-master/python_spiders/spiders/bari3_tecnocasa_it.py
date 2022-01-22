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

class Bari3_tecnocasaSpider(scrapy.Spider):
        
    name = 'bari3_tecnocasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['bari3.tecnocasa.it']
    start_urls = ['https://bari3.tecnocasa.it/appartamenti-in-affitto']

    position = 1

    def parse(self, response):

            
        cards = response.css(".immobiliListaAnnunci .estate-card")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css("a.hover-link::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            

            
            Bari3_tecnocasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination a:contains('>')::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)

    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("#modalFeatures .modal-body .col-md-6:contains('Rif')::text").getall()
        external_id = " ".join(external_id)
        if external_id:
            external_id = remove_white_spaces(external_id).replace(":","")

        square_meters = response.css(".estate-card-surface img::attr(alt)").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".estate-card-rooms img::attr(alt)").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".estate-card-bathrooms img::attr(alt)").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".estate-card-current-price::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".estate-card-current-price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".schedaAnnuncioTitolo h1 strong::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address = response.css(".schedaAnnuncioSottoTitolo::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = address.split(", ")[0]
            
        

        script_map = response.css("detail-map").get()
        if script_map:
            pattern = re.compile(r':latitude="(\d*\.?\d*)" :longitude="(\d*\.?\d*)"')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']

    
        description = response.css("#modalDescription .modal-body p::text, #modalDescription .modal-body p strong::text, #modalDescription .modal-body p strong a::text, #modalDescription .modal-body p a::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.img-grid-item img::attr(src)').getall()
        external_images_count = len(images)

 
        energy_label = response.css(".square.active span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        utilities = response.css(".schedaAnnuncioCampi div:contains('Spese') + div::text").get()
        if utilities:
            utilities = int(extract_number_only(utilities).replace(".","")) // 12
        
        floor = response.css("#modalFeatures .modal-body .col-md-6:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor).replace(":","")
        

        elevator = response.css(".tag:contains('ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  
            
        balcony = response.css(".tag:contains('balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
        furnished = response.css("#modalFeatures .modal-body .col-md-6:contains('Arredamento')::text").getall()
        furnished = " ".join(furnished)
        if "Assente" in furnished:
            furnished = False
        elif furnished:
            furnished = True
        else:
            furnished = False
        
        landlord_email = "bahd1@tecnocasa.it"
        landlord_phone = "0809081033"
        landlord_name = response.css(".datiAffiliato strong::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)

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
            item_loader.add_value("utilities", utilities)
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
