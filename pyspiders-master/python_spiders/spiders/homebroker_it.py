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

class HomebrokerSpider(scrapy.Spider):
        
    name = 'homebroker'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.homebroker.it']

    position = 1

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.homebroker.it/r/annunci/affitto-appartamento-.html?Codice=&Tipologia%5B%5D=1&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Locali_da=&Locali_a=&Camere_da=&Camere_a=&cf=yes&ordineImmobili=prezzoMIN&p=0',
            'property_type': 'apartment'},
            {'url': 'https://www.homebroker.it/r/annunci/affitto-terratetto-.html?Codice=&Tipologia%5B%5D=36&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Locali_da=&Locali_a=&Camere_da=&Camere_a=&cf=yes&ordineImmobili=prezzoMIN&p=0',
            'property_type': 'house'},
            {'url': 'https://www.homebroker.it/r/annunci/affitto-villa-.html?Codice=&Tipologia%5B%5D=9&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Locali_da=&Locali_a=&Camere_da=&Camere_a=&cf=yes&ordineImmobili=prezzoMIN&p=0',
            'property_type': 'house'},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta={'property_type': url.get('property_type')})

    def parse(self, response):
        

        
        cards = response.css("ul.realestate .realestate-lista")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = response.meta['property_type']
            
            card_url = card.css("a::attr(href)").get()

            external_id = card.css(".codice-list::text").get()
            if external_id:
                external_id = extract_number_only(remove_white_spaces(external_id))
                        
            rent = card.css(".prezzo_full-list::text").get()
            if rent:
                rent = extract_number_only(rent).replace(".","")


            currency = card.css(".prezzo_full-list::text").get()
            if currency:
                currency = remove_white_spaces(currency)
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR"
            
            room_count = card.css(".details-list .ico-24-locali::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
            else: 
                room_count = card.css(".details-list .ico-24-camere::text").get()
                if room_count:
                    room_count = remove_white_spaces(room_count)
                    room_count = extract_number_only(room_count)


            bathroom_count = card.css(" .details-list .ico-24-bagni::text").get()
            if bathroom_count:
                bathroom_count = remove_white_spaces(bathroom_count)
                bathroom_count = extract_number_only(bathroom_count) 
            else:
                bathroom_count = 1
            
            energy_label = card.css(".classificazione span + div::text").get()
            if energy_label:
                energy_label = remove_white_spaces(energy_label)
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "external_id": external_id,
                "rent": rent,
                "currency": currency,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "energy_label": energy_label,
            }            
            
            HomebrokerSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        if len(cards) > 0:
            prev_page = int(parse_qs(response.url)['p'][0])
            next_page = int(parse_qs(response.url)['p'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&p={prev_page}",f"&p={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
        
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True, meta = response.meta)
      


    def parseApartment(self, response):


        square_meters = 0
        
        title = response.css("head title::text").get()
        if title:
            title = remove_white_spaces(title)

        
        
        Regione = response.css('#sezInformazioni div:contains("Regione")::text').get()
        if Regione:
            Regione = remove_white_spaces( Regione.replace(":","") )
            
        Comune = response.css('#sezInformazioni div:contains("Comune")::text').get()
        if Comune:
            Comune = remove_white_spaces( Comune.replace(":","") )
            
        Provincia = response.css('#sezInformazioni div:contains("Provincia")::text').get()
        if Provincia:
            Provincia = remove_white_spaces( Provincia.replace(":","") )
            
        address = f"{Provincia} {Comune} {Regione}"

        
        
        description = response.css('.testo p::text,.testo p strong::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        

        
        images = response.css('.swiper-slide img::attr(data-src)').getall()
        external_images_count = len(images)
        

        
        landlord_email = "info@homebroker.it"
        landlord_phone = "0556587322"
        landlord_name = "Home Broker di Filippo Curradi"

        

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta['external_id'])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", Comune)
        item_loader.add_value("address", address)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("energy_label", response.meta['energy_label'])
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
