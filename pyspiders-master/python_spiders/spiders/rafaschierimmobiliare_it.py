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

class RafaschierimmobiliareSpider(scrapy.Spider):
        
    name = 'rafaschierimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.rafaschierimmobiliare.it']
    post_url = 'http://www.rafaschierimmobiliare.it/immobili.php'
    
    position = 1
    formdata = {
        "chk": "1",
        "o": "1",
        "t": "1",
        "v": "0",
        "p1": "0,00 €",
        "p2": "9.999.999,00 €",
        "r": "",
        "c": "0",
        "z": "Area",
    }
    
    def start_requests(self):
        yield FormRequest(
                        url = self.post_url, 
                        formdata = self.formdata, 
                        callback = self.parse, 
                        dont_filter = True
                        )


    def parse(self, response):
        
        cards = response.css(".spacing-list")

        for index, card in enumerate(cards):


            rent = card.css(".item-prop-desc .table th:contains('Prezzo') + td::text").get()
            if rent and "Tratt. Riservate" not in rent:
                rent = extract_number_only(rent).replace(".","")
            else:
                continue
                    
            currency = card.css(".item-prop-desc .table th:contains('Prezzo') + td::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR" 

            
            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a.item-prop::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "rent": rent,
                "currency": currency,
            }

            RafaschierimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        





    def parseApartment(self, response):


        external_id = response.css("h4:contains('Rif:')::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)
        
        square_meters = response.css(".det-tab-title:contains('Superfice') + span").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".det-tab-title:contains('Vani') + span").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1


        title = response.css("li[role='presentation'] a::text").get()
        if title:
            title = remove_white_spaces(title)
        
                        
        city = response.css(".det-tab-title:contains('Città') + span::text").get()
        if city:
            city = remove_white_spaces(city)
            
        zona = response.css(".det-tab-title:contains('Zona') + span::text").get()
        if zona:
            zona = remove_white_spaces(zona)
        
        street = response.css(".det-tab-title:contains('Indirizzo') + span::text").get()
        if street:
            street = remove_white_spaces(street)
            
        address = f"{city} - {zona} - {street}"
            
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'var myLatLng = {lat: (\d*\.?\d*), lng:  (\d*\.?\d*)};')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

    
        description = response.css('.det-desc::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css("[data-u='slides'] img[data-u='image']::attr(src)").getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        energy_label = response.css(".det-tab-title:contains('Energetica') + span").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        

        elevator = response.css(".det-accessori li:contains('Ascensore')::attr(class)").get()
        if "on" in elevator:
            elevator = True
        elif "off" in elevator:
            elevator = False


        balcony = response.css(".det-accessori li:contains('Balcon')::attr(class)").get()
        if "on" in balcony:
            balcony = True
        elif "off" in balcony:
            balcony = False

        
        landlord_name = "rafaschier immobiliare"
        landlord_phone = "+390805212959"
        landlord_email = "info@rafaschierimmobiliare.it"
                

        
        if response.meta['rent']:
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
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", response.meta['currency'])
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
