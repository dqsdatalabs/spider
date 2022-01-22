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

class GetmihomeSpider(scrapy.Spider):
        
    name = 'getmihome'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.getmihome.it']
    start_urls = ['https://getmihome.it/ricerca/']

    position = 1

    def parse(self, response):
            
        cards = response.css(".property_listing")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css("h4 a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            GetmihomeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("#propertyid_display::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css(".listing_detail:contains('Superficie')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".listing_detail:contains('Locali')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".listing_detail:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css(".notice_area .price_area::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".notice_area .price_area::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h1.entry-title::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        address = response.css(".property_categs::text, .property_categs a::text").getall()
        address = " ".join(address)
        if address:
            address = remove_white_spaces(address)
            
        city = address.split(", ")[1]

            
        latitude = response.css("#googleMap_shortcode::attr(data-cur_lat)").get()
        longitude = response.css("#googleMap_shortcode::attr(data-cur_long)").get()
        
    
        description = response.css(".wpestate_property_description p::text, .wpestate_property_description p strong::text, .wpestate_property_description p strong a::text, .wpestate_property_description p a::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.owl-carousel img::attr(src)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.floor_image img::attr(src)').getall()
 
        energy_label = response.css(".listing_detail:contains('energetico')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split("-")[0]
        
    
        washing_machine = response.css(".listing_detail:contains('Lavatrice')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False 
        
        dishwasher = response.css(".listing_detail:contains('Lavastoviglie')::text").get()
        if dishwasher:
            dishwasher = True
        else:
            dishwasher = False 
        
        
        landlord_email = response.css(".agent_email_class a::text").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
        
        landlord_phone = response.css(".agent_mobile_class a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")

        landlord_name = response.css(".agent_details h3 a::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)

        available_date = response.css(".slider-property-status::text").get()
        if available_date:
            available_date = remove_white_spaces(available_date)
            regex_pattern = r"Da (?P<month>(\w+)) (?P<year>(\d+))"
            regex = re.compile(regex_pattern)
            match = regex.search(available_date)
            months = ['Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre']
            if match:
                available_date = f"01/{months.index(match['month']) + 1}/{match['year']}"
            available_date = format_date(available_date)
        else:
            available_date = None

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
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images",floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
