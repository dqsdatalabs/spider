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

class ImmobiliaregiannottifirenzeSpider(scrapy.Spider):
        
    name = 'immobiliaregiannottifirenze'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliaregiannottifirenze.it']
    start_urls = ['https://immobiliaregiannottifirenze.it/elenco_immobili_f.asp?idm=1415&idcau2=1']

    position = 1

    def parse(self, response):
        
        
        cards = response.css(".aa-properties-nav li")
        
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("a.aa-secondary-btn::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                

            # external_id = card.css(".rif::text").get()
            # if external_id:
            #     external_id = remove_white_spaces(external_id).split("Rif. ")[1]
            
            external_id = extract_number_only(card_url).replace(".","")
            # if external_id:
            #     external_id = remove_white_spaces(external_id).split("Rif. ")[1]


            rent = card.css(".aa-price::text").get()
            if rent:
                rent = extract_number_only(rent).replace(".","")
            else:
                rent = None
                
            currency = card.css(".aa-price::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR"

            square_meters = card.css(".aa-properties-info span:contains('Mq')::text").get()
            if square_meters:
                square_meters = remove_white_spaces(square_meters)
                square_meters = extract_number_only(square_meters)
            
            room_count = card.css(".aa-properties-info span:contains('Locali')::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
            else:
                room_count = 1

            bathroom_count = 1

            city = card.css(" .aa-properties-about h3 a::text").get()
            if city:
                city = remove_white_spaces(city).split(" ")[1]
            
            
            address = city
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "external_id": external_id,
                "rent": rent,
                "currency": currency,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "city": city,
                "address": address,
            }
            
            
            
            ImmobiliaregiannottifirenzeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css("a[aria-label='Next']::attr(href)").get()
        if nextPageUrl and nextPageUrl != "#elenco_imm":
            nextPageUrl = response.urljoin(nextPageUrl)
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      


    def parseApartment(self, response):
        
        title = response.css(".aa-properties-info h2::text").get()
        if title:
            title = remove_white_spaces(title)
            
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r"var myLatlng = new google.maps.LatLng\('(\d*\.?\d*)', '(\d*\.?\d*)'\);")
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
    
        description = response.css('.descrizione_singolo_annuncio::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.bxslider li a::attr(href)').getall()
        external_images_count = len(images)
        

        energy_label = response.css(".description-list li:contains('Energetica') span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)

        
        floor = response.css("ul li:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor).split(": ")[1]

            
        elevator = response.css("ul li:contains('Ascensore')::text").get()
        if elevator:
            if "NO" in elevator:
                elevator = True
            elif "SI" in elevator:
                elevator = False
            else:
                elevator = False 
          
        balcony = response.css("ul li:contains('Balcone')::text").get()
        if balcony:
            if "NO" in balcony:
                balcony = True
            elif "SI" in balcony:
                balcony = False
            else:
                balcony = False   
        
        terrace = response.css("ul li:contains('Terrazzo')::text").get()
        if terrace:
            if "NO" in terrace:
                terrace = True
            elif "SI" in terrace:
                terrace = False
            else:
                terrace = False   
       
        
        landlord_email = "info@immobiliaregiannottifirenze.it"
        landlord_phone = "0556811985"
        landlord_name = "REAL ESTATE GIANNOTTI FLORENCE"        
        
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("address", response.meta['address'])
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
