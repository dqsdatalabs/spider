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

class Monti32Spider(scrapy.Spider):

    name = 'monti32'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.monti32.it']
    start_urls = ['https://monti32.it/affitto-milano.html']

    position = 1
    
    def parse(self, response):
        
        cards = response.css(".container-schede .box-scheda")
        

        for index, card in enumerate(cards):

            position = self.position
            property_type = "apartment"
            city = "Milan"
            
            card_url = card.css("a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
            
            external_id = card.css("a .rif.sei::text").get()
            if external_id:
                external_id = external_id.split(":")[1].strip()

            square_meters = card.css("a .size.sei::text").get()
            if square_meters:
                square_meters = square_meters.split(":")[1].strip()
                if square_meters == "0":
                    square_meters = 30 
            

            rent = card.css("a .price.sei::text").get()
            if rent:
                rent = remove_white_spaces(rent)
                rent = convert_string_to_numeric(rent, Monti32Spider)

            currency = card.css("a .price.sei::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
                
     

            dataUsage = {
                "position": position,
                "property_type": property_type,
                "city": city,
                "card_url": card_url,
                "external_id": external_id,
                "square_meters": square_meters,
                "rent": rent,
                "currency": currency,
            }
            
            
            Monti32Spider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagine a:contains('>')::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)
            
        
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter=True)
        

    def parseApartment(self, response):

        room_count = response.css(".info-container .sum-table td:contains('Camere') + td::text").get()
        if room_count:
            room_count = convert_string_to_numeric(room_count, Monti32Spider)
        else:
            room_count = 1 

        bathroom_count = response.css(".info-container .sum-table td:contains('Bagni') + td::text").get()
        if bathroom_count:
            bathroom_count = convert_string_to_numeric(bathroom_count, Monti32Spider)
        else:
            bathroom_count = 1 
        

        location = response.css("#latlng::attr(value)").get()
        if location:
            latitude = location.split(",")[0]
            longitude = location.split(",")[1]
        
        title = response.css("head title::text").get()
        if title:
            title = title.strip()
            
        address = response.css(".detail-title h1::text").get()
        if address:
            address = address

        
        description = response.css('.detail-text::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

            
        energy_label = response.css(".info-container .sum-table td:contains('Classe Energetica') + td::text").get()
        if energy_label:
            energy_label = energy_label.strip()

        images = response.css('.fotorama img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        floor_plan_images = response.css('#planimetrie img::attr(src)').getall()
        floor_plan_images = [response.urljoin(img) for img in floor_plan_images]


        
        landlord_email = "info@monti32.it"
        landlord_phone = "+39024984412"
        landlord_name = "monti32 immobili"


        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images",floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
        pass

def get_p_type_string(p_type_string):

    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
