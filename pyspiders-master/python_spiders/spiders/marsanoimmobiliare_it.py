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

class MarsanoimmobiliareSpider(scrapy.Spider):
        
    name = 'marsanoimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.marsanoimmobiliare.it']
    start_urls = ['https://www.marsanoimmobiliare.it/affitti/']

    position = 1

    def parse(self, response):
        
        
        cards = response.css("[data-tab='1'] .property-content .property-item")
        

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(" .property-inner .property-title a::attr(href)").get()

            
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
        
            
            
            MarsanoimmobiliareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
      






    def parseApartment(self, response):

        property_type = "apartment"
        
        external_id = response.css("link[rel='shortlink']::attr(href)").get()
        if external_id:
            external_id = remove_white_spaces(external_id).split("?p=")[1]
        
        square_meters = response.css(".property-area .property-info-value::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
        
        room_count = response.css(".property-location .ere-property-list li strong:contains('Vani') + span::text").get()
        if room_count:
            room_count = convert_string_to_numeric(room_count, MarsanoimmobiliareSpider)
        else:
            room_count = 1 

        bathroom_count = response.css(".property-location .ere-property-list li strong:contains('Bagni') + span::text").get()
        if bathroom_count:
            bathroom_count = convert_string_to_numeric(bathroom_count, MarsanoimmobiliareSpider)
        else:
            bathroom_count = 1             

        rent = response.css(".property-info-block-inline .property-price::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(",","")
            rent = convert_string_to_numeric(rent, MarsanoimmobiliareSpider)

        currency = response.css(".property-info-block-inline .property-price::text").get()
        if currency:
            currency = remove_white_spaces(currency).replace(",","")
            currency = currency_parser(currency, self.external_source)
        
        title = response.css(".property-heading h4::text").get()
        if title:
            title = remove_white_spaces(title)
            
        street = response.css(".property-location li strong:contains('Indirizzo') + span::text").get()
        quarter = response.css(".property-location li strong:contains('Quartiere') + span::text").get()
        city = response.css(".property-location li strong:contains('Città') + span::text").get()
        address = f"{street}, {quarter}, {city}"
        
        description = response.css('.property-description p::text, .property-description p a::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
  
        energy_label = response.css(".property-location .ere-property-list li strong:contains('CE') + span::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split(";")[0]
        
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'var property_position = new google.maps.LatLng\((\d*\.?\d*), (\d*\.?\d*)\);')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
            
        
        images = response.css('.single-property-image-main .property-gallery-item a::attr(href)').getall()
        external_images_count = len(images)
        
        floor_plan_images = response.css('.floor-image img::attr(src)').getall()

        floor = response.css(".property-location .ere-property-list li strong:contains('Piano') + span::text").get()
        if floor:
            floor = remove_white_spaces(floor)
            
        utilities = response.css(".property-location .ere-property-list li strong:contains('Spese') + span::text").get()
        if utilities:
            utilities = extract_number_only(utilities).split(".")[0]


        elevator = response.css(".property-location .ere-property-list li strong:contains('Ascensore') + span::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "no":
                elevator = False
            elif elevator == "si":
                elevator = True


        landlord_name = "Studio Immobiliare Marsano"
        landlord_email = "info@marsanoimmobiliare.it"
        landlord_phone = "+390103748424"

        
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
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images",floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
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
