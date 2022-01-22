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

class Gallery_immobiliareSpider(scrapy.Spider):
        
    name = 'gallery_immobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.gallery-immobiliare.it']
    start_urls = ['https://www.gallery-immobiliare.it/immobili/affitto/']

    position = 1

    def parse(self, response):
        
        
        
        cards = response.css(".POIPO")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css(".POIPO .phone.info-row a::attr(href)").get()
                

            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            Gallery_immobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagination a[rel='Next']::attr(href)").get()
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      






    def parseApartment(self, response):


        external_id = response.css(".detail-amenities-list li.media .media-body:contains('ID Immo')::text").getall()
        external_id = " ".join(external_id)
        if external_id:
            external_id = remove_white_spaces(external_id).split("ID Immobile")[1]
            external_id = remove_white_spaces(external_id)
        
        if external_id == "":
            external_id = response.css("link[rel='shortlink']::attr(href)").get().split("?p=")[1]

        
        square_meters = response.css(".detail-amenities-list li.media .media-body:contains('Dimension')::text").getall()
        square_meters = " ".join(square_meters)
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".detail-amenities-list li.media .media-body:contains('Camer')::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".detail-amenities-list li.media .media-body:contains('Bagn')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css("span.item-price::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css("span.item-price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".table-cell h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        country = response.css(".detail-country::text").getall()
        country = " ".join(country)
        if country:
            country = remove_white_spaces(country)
            
        state = response.css(".detail-state::text").getall()
        state = " ".join(state)
        if state:
            state = remove_white_spaces(state)
            
        city = response.css(".detail-city::text").getall()
        city = " ".join(city)
        if city:
            city = remove_white_spaces(city)
            
        comune= response.css("address.property-address::text").get()
        if comune :
            comune  = remove_white_spaces(comune)
                        
        address = f"{comune} - {city} - {state} - {country}"
            
        script_map = response.css("script#houzez_ajax_calls-js-extra::text").get()
        if script_map:
            pattern = re.compile(r',"property_lat":"(\-?\d*\.?\d*)","property_lng":"(\-?\d*\.?\d*)",')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
    
        description = response.css('.detail-title ~ p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.detail-gallery a::attr(href)').getall()
        external_images_count = len(images)
           

        elevator = response.css(".detail-features a:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False
        
             
            
        swimming_pool = response.css(".detail-features a:contains('Piscina')::text").get()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False   

        parking = response.css(".detail-features a:contains('Posto')::text").get()
        if parking:
            parking = True
        else:
            parking = False   
             
        
        terrace = response.css(".detail-features a:contains('Terrazza')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False    
        
        washing_machine = response.css(".detail-features a:contains('Lavatrice')::text,.detail-features a:contains('Lavanderia')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False    
        
        landlord_email = "info@gallery-immobiliare.it"
        landlord_phone = "+393356259556"
        landlord_name = "Gallery Immobiliare"
                
  
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
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
