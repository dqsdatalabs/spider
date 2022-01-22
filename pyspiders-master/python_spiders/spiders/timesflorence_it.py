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

class TimesflorenceSpider(scrapy.Spider):
    
    name = 'timesflorence'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.timesflorence.it']
    start_urls = ['https://www.timesflorence.it/action/affitto/']

    position = 1

    def parse(self, response):
        
        cards = response.css(".property_listing.property_card_default")
        

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".property_listing_details a::attr(href)").get()

            
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            TimesflorenceSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagination.pagination_nojax .roundright a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = nextPageUrl
            
        
        if nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter=True)
      


    def parseApartment(self, response):

        property_type = "apartment"

        external_id = response.css(".rightmargin #details .listing_detail:contains('ID proprietà')::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css(".rightmargin #details .listing_detail:contains('Dimensioni proprietà')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters).split(" ")[0].split(".")[0]
        
        room_count = response.css(".rightmargin #details .listing_detail:contains('Vani')::text").get()
        if room_count:
            room_count = convert_string_to_numeric(room_count, TimesflorenceSpider)
        else:
            room_count = 1 

        bathroom_count = response.css(".rightmargin #details .listing_detail:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = convert_string_to_numeric(bathroom_count, TimesflorenceSpider)
        else:
            bathroom_count = 1             

        rent = response.css(".notice_area .price_area::text").get()
        if rent:
            rent = remove_white_spaces(rent)
            rent = convert_string_to_numeric(rent, TimesflorenceSpider)

        currency = response.css(".notice_area .price_area::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        
        title = response.css(".notice_area .entry-title::text").get()
        if title:
            title = remove_white_spaces(title)
            
        address = response.css(".notice_area .property_categs::text,.notice_area .property_categs a::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)

        city = address.split(',')[-1]
        if city:
            city = remove_white_spaces(city)
        
        description = response.css('.rightmargin #description > p::text, .rightmargin #description > p span::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
  
        energy_label = response.css(".rightmargin #details .listing_detail:contains('energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        latitude = response.css("#googleMap_shortcode::attr(data-cur_lat)").get()
        if latitude:
            latitude = latitude
        
        longitude = response.css("#googleMap_shortcode::attr(data-cur_long)").get()
        if longitude:
            longitude = longitude
        
        images = response.css('#carousel-listing .carousel-inner .item img::attr(src)').getall()
        # images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        

        floor = response.css(".rightmargin #details .listing_detail:contains('Piano')::text").get()
        if floor:
            floor = remove_white_spaces(floor)
            
        
        elevator = response.css(".rightmargin #details .listing_detail:contains('Ascensore')::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "non disponibile":
                elevator = False
            elif elevator == "no":
                elevator = False
            elif elevator == "si":
                elevator = True
            elif elevator == "yes":
                elevator = True

        
        balcony = response.css(".rightmargin #details .listing_detail:contains('Balcone')::text").get()
        if balcony:
            balcony = remove_white_spaces(balcony).lower()
            if balcony == "non disponibile":
                balcony = False
            elif balcony == "no":
                balcony = False
            elif balcony == "si":
                balcony = True
            elif balcony == "yes":
                balcony = True
            elif int(balcony) != 0:
                balcony = True

        terrace = response.css(".rightmargin #details .listing_detail:contains('Terrazzo')::text").get()
        if terrace:
            terrace = remove_white_spaces(terrace).lower()
            if terrace == "non disponibile":
                terrace = False
            elif terrace == "no":
                terrace = False
            elif terrace == "si":
                terrace = True
            elif terrace == "yes":
                terrace = True

        landlord_name = "Times - Athena Real estate management"
        landlord_email = "times@timesflorence.it"
        landlord_phone = "+39055353066"
        
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
