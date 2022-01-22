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

class FontebuoniSpider(scrapy.Spider):
        
    name = 'fontebuoni'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.fontebuoni.com']

    position = 1

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.fontebuoni.it/it/ricerca-immobili.php?q=ricerca&tp=2&ar=1&ta=&cm=&zn=&superficieMinima=&superficieMassima=&vaniMinimo=&vaniMassimo=&prezzoMinimo=&prezzoMassimo=",
                'property_type': 'apartment'
            },
            {
                "url": "https://www.fontebuoni.it/it/ricerca-immobili.php?q=ricerca&tp=2&ar=2&ta=&cm=&zn=&superficieMinima=&superficieMassima=&vaniMinimo=&vaniMassimo=&prezzoMinimo=&prezzoMassimo=",
                'property_type': 'house'
            },
        ]

        for url in start_urls:

            yield Request(url = url.get('url'), 
                          callback = self.parse, 
                          dont_filter = True, 
                          meta = {'property_type': url.get('property_type')})


    def parse(self, response):
        
        
        
        cards = response.css("#property-listing .row .item")
        

        for index, card in enumerate(cards):

            position = self.position
            property_type = response.meta['property_type']
            card_url = card.css(".image a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            FontebuoniSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagination #next a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta = response.meta)
      

    def parseApartment(self, response):


        external_id = response.css("#property-id::text").get()
        if external_id:
            external_id = external_id.split(" ")[1]
        
        square_meters = response.css(".amenities li:nth-of-type(2)::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".amenities li:nth-of-type(3)::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".amenities li:nth-of-type(4)::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)      

        rent = response.css(".price span::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")


        currency = response.css(".price span::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("head title::text").get()
        if title:
            title = remove_white_spaces(title)
        
        address = response.css(".property-title small::text").get()
        if address:
            address = remove_white_spaces(address)

        city = address.split("zona")[0].replace("a","").replace(" ","")
        
        description = response.css('#property-detail-wrapper + p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        script_map = response.css("#property_location script::text").get()
        if script_map:
            pattern = re.compile(r'var mapCenter = new google.maps.LatLng\((\d*\.?\d*), (\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
    
        
        images = response.css('#property-detail-large img::attr(src)').getall()
        external_images_count = len(images)
        
        energy_label = response.css(".property-amenities-list li:contains('Energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(remove_white_spaces(energy_label).split(":")[1])
        
        floor = response.css(".property-amenities-list li:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(remove_white_spaces(floor).split(":")[1])

        elevator = response.css(".property-amenities-list li:contains('Ascensore')::attr(class)").get()
        if elevator == "enabled":
            elevator = True
        elif elevator == "disabled":
            elevator = False
        else:
            elevator = False
         
        balcony = response.css(".property-amenities-list li:contains('Balconi')::attr(class)").get()
        if balcony == "enabled":
            balcony = True
        elif balcony == "disabled":
            balcony = False
        else:
            balcony = False   

        terrace = response.css(".property-amenities-list li:contains('Terrazzi')::attr(class)").get()
        if terrace == "enabled":
            terrace = True
        elif terrace == "disabled":
            terrace = False
        else:
            terrace = False   
        
        

        
        landlord_email = response.css(".agent-detail .contact-us li i.fa-envelope + a::text").get()
        if landlord_email:
            landlord_email = landlord_email
        
        landlord_phone = response.css(".agent-detail .contact-us li i.fa-phone + a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)

        landlord_name = response.css(".agent-detail h2::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)


        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
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
