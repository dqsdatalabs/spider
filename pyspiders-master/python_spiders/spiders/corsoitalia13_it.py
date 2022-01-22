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

class Corsoitalia13Spider(scrapy.Spider):
        
    name = 'corsoitalia13'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.corsoitalia13.it']
    start_urls = ['https://www.corsoitalia13.it/immobili-in-affitto/']

    position = 1

    def parse(self, response):

        cards = response.css(".listing_wrapper")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css("::attr(data-modal-link)").get()
                  
            external_id = card.css("::attr(data-listid)").get()
            title = card.css("::attr(data-modal-title)").get()
            
            dataUsage = {
                "position": position,
                "card_url": card_url,
                "external_id": external_id,
                "title": title,
            }
            
            
            
            Corsoitalia13Spider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css(".pagination .roundright a::attr(href)").get()

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "apartment"
            

        square_meters = response.css(".listing_detail:contains('Dimensioni Proprietà:')::text").getall()
        square_meters = " ".join(square_meters)
        if square_meters:
            square_meters = remove_white_spaces(square_meters).split(".")[0]
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".listing_detail:contains('Camere:')::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".listing_detail:contains('Bagni')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".price_area::text").get()
        if rent:
            rent = remove_white_spaces(rent)
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".price_area::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
           
        address = response.css(".listing_detail:contains('Address:')::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = response.css(".listing_detail:contains('City:') a::text").get()
        if city:
            city = remove_white_spaces(city)
            
        zipcode = response.css(".listing_detail:contains('Zip:')::text").getall()
        zipcode = " ".join(zipcode)
        if zipcode:
            zipcode = remove_white_spaces(zipcode)
            
        latitude = response.css("#gmap_wrapper::attr(data-cur_lat)").get()
        longitude = response.css("#gmap_wrapper::attr(data-cur_long)").get()

        description = response.css(".panel-title:contains('Descrizione') + p::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('#property_slider_carousel a::attr(href)').getall()
        external_images_count = len(images)

 
        energy_label = response.css(".listing_detail:contains('Energy class:')::text").getall()
        energy_label = " ".join(energy_label)
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        
        floor = response.css(".listing_detail:contains('Piano:')::text").getall()
        floor = " ".join(floor)
        if floor:
            floor = remove_white_spaces(floor).replace(":","")
        
        
        landlord_email = response.css(".agent_email_class a::text").get()
        landlord_phone = response.css(".agent_phone_class a::text").get().replace(" ","")
        landlord_name = response.css(".agent_details h3 a::text").get()


        if rent: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta['external_id'])
            item_loader.add_value("title", response.meta['title'])
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
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("floor", floor)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
