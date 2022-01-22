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

class ImmobiliareferroniSpider(scrapy.Spider):
        
    name = 'immobiliareferroni'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliareferroni.com']
    start_urls = ['https://www.immobiliareferroni.com/immobili-verona/']

    position = 1
    def parse(self, response):
            
        cards = response.css(".grid-offer-col")

        for index, card in enumerate(cards):


            card_type = card.css(".offer-status:contains('Vendita')::text").get()
            if card_type:
                continue

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css(".grid-offer-back .button a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            
            ImmobiliareferroniSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css("a.active + a::attr(href)").get()

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)



    def parseApartment(self, response):

        external_id = response.css(".desc-parameters-name:contains('Riferimento') + .desc-parameters-val::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css(".details-parameters-name:contains('Mq') + .details-parameters-val::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".details-parameters-name:contains('Stanze') + .details-parameters-val::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".details-parameters-name:contains('Bagni') + .details-parameters-val::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".details-parameters-price::text").get()
        if rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".details-parameters-price::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".details-title h3::text").get()
        if title:
            title = remove_white_spaces(title)
         
            
        address = response.css(".desc-parameters-name:contains('Address') + .desc-parameters-val::text").get()
        if address:
            address = remove_white_spaces(address)     
            
        
        zipcode = extract_number_only(address)
        if zipcode == "18":
            zipcode = "37121" 
        
        
        script_map = response.css("script#footer-js-js-extra::text").get()
        if script_map:
            pattern = re.compile(r',"estate_map":"(\d*\.?\d*),(\d*\.?\d*),')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        city = responseGeocodeData['address']['City']
    
        description = response.css('.details-desc p span::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.swiper-wrapper .swiper-slide::attr(data-src)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.plans-gallery::attr(href)').getall()
 
        energy_label = response.css(".details-parameters-name:contains('classe')::text, .details-parameters-name:contains('Classe')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split(" ")[-1]
        
        floor = response.css(".details-parameters-name:contains('Piano') + .details-parameters-val::text").get()
        if floor:
            floor = remove_white_spaces(floor)
        
        available_date = response.css(".details-parameters-name:contains('Da:') + .details-parameters-val::text").get()
        if available_date:
            available_date = remove_white_spaces(available_date)

        elevator = response.css(".details-ticks li:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False  
        
        furnished = response.css(".details-ticks li:contains('Arredato')::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False  
              
        parking = response.css(".details-ticks li:contains('Posto Auto')::text, .details-ticks li:contains('Parcheggio Pubblico')::text").get()
        if parking:
            parking = True
        else:
            parking = False     
        
        landlord_email = "info@immobiliareferroni.com"
        landlord_phone = "+393469577009"
        landlord_name = "IMMOBILIARE FERRONI DI FERRONI MICHELE"


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
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
