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

class BarimmobiliareSpider(scrapy.Spider):
        
    name = 'barimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.barimmobiliare.com']
    start_urls = ['http://barimmobiliare.com/home/index.php/search-results/?sort=newest&search_city=&search_lat=&search_lng=&search_category=7&search_type=8&search_bedrooms=0&search_bathrooms=0']

    position = 1

    def parse(self, response):
        
        
        cards = response.css(".resultsList .row div.col-lg-6")
        

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css("a::attr(href)").get()

            
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            BarimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css("ul.pagination li.active + li a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = nextPageUrl
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter=True)
      


    def parseApartment(self, response):

        property_type = "apartment"

        external_id = response.css(".property-id::text").get()
        if external_id:
            external_id = extract_number_only(remove_white_spaces(external_id))
        
        square_meters = response.css(".summaryItem .features li span.icon-frame + div::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".summaryItem .features li span.fa-moon-o + div::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)


        bathroom_count = response.css(".summaryItem .features li span.icon-drop + div::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
          

        rent = response.css(".summaryItem .listPrice::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")


        currency = response.css(".summaryItem .listPrice::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".pageTitle::text").get()
        if title:
            title = remove_white_spaces(title)
        
        address = response.css('.summaryItem .address::text').getall()
        address = " ".join(address)
        address = remove_white_spaces(address)
        
        
        description = response.css('.description .entry-content p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        
        action = "reales_get_single_property"
        single_id = response.css('#single_id::attr(value)').get()
        security = response.css('#securityAppMap::attr(value)').get()
        responseSomeData = requests.post("http://barimmobiliare.com/home/wp-admin/admin-ajax.php",
                                         data = {
                                                "action": action,
                                                "single_id": single_id,
                                                "security": security,
                                                })
        dataResponseSomeData = responseSomeData.json()["props"][0]
        
        latitude = dataResponseSomeData["lat"]
        longitude = dataResponseSomeData["lng"]
        city = dataResponseSomeData["city"]
        zipcode = dataResponseSomeData["zip"]
    
        
        images = response.css('.carousel-inner a::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.floorPlans img::attr(src)').getall()


        energy_label = response.css(".additional .amItem:contains('Energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split(" ")[1]
            
        
        floor = response.css(".additional .amItem:contains('Piano')::text").get()
        if floor:
            floor = floor.lower()
            floor = remove_unicode_char(remove_white_spaces(floor))
            if "piano" in floor:
                floor = 0
            else: 
                floor = extract_number_only(floor)
           
        utilities = response.css(".additional .amItem:contains('Condominiale')::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        elevator = response.css(".amenities div.amItem:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False

        balcony = response.css(".amenities div.amItem:contains('Balcone')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False
        
        landlord_email = response.css("#agent_email::attr(value)").get()
        if landlord_email:
            landlord_email = landlord_email
        
        landlord_phone = response.css(".agentName::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).split("Cell")[1]
            landlord_phone = extract_number_only(landlord_phone)

        landlord_name = response.css(".agentName::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name).split("-")[0]
        

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
        item_loader.add_value("floor_plan_images",floor_plan_images)
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
