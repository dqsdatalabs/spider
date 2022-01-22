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

class BortolaiSpider(scrapy.Spider):
        
    name = 'bortolai'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.bortolai.it']
    start_urls = ['https://www.bortolai.it/it/affitto.html']

    position = 1

    def parse(self, response):
        
        cards = response.css(".elementor-column-wrap .dl-service")
        

        for index, card in enumerate(cards):

            position = self.position
            property_type = "apartment"
            card_url = card.css("a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            BortolaiSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css(".pagination .next::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)
            
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)
      




    def parseApartment(self, response):


        external_id = response.css(".p-info-list2 li .p-label:contains('Riferimento') + .p-value::text").get()
        if external_id:
            external_id = external_id
        
        square_meters = response.css(".p-info-list li .p-label:contains('Superficie') + .p-value::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".p-info-list li .p-label:contains('vani') + .p-value::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".p-info-list li .p-label:contains('Bagni') + .p-value::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
        else:
            bathroom_count = 1             

        rent = response.css(".p-info-list2 li .p-label:contains('Prezzo') + .p-value b::text").get()
        if rent:
            rent = extract_number_only(rent)
            
        currency = "EUR"
        
        title = response.css(".project-info-block .project-info-box h2::text").get()
        if title:
            title = remove_white_spaces(title)
        
        
        Regione = response.css(".p-info-list2 li .p-label:contains('Regione') + .p-value::text").get()
        if Regione:
            Regione = remove_white_spaces(Regione)
        Provincia = response.css(".p-info-list2 li .p-label:contains('Provincia') + .p-value::text").get()
        if Provincia:
            Provincia = remove_white_spaces(Provincia)
        Comune = response.css(".p-info-list2 li .p-label:contains('Comune') + .p-value::text").get()
        if Comune:
            Comune = remove_white_spaces(Comune)
        Indirizzo = response.css(".p-info-list2 li .p-label:contains('Indirizzo') + .p-value::text").get()
        if Indirizzo:
            Indirizzo = remove_white_spaces(Indirizzo)
        
        city = Comune
        address = f"{Regione} - {Provincia} - {Comune} - {Indirizzo}"
        
        
        script_map = response.css(".project-info-block .project-info-box script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'positionMarker = new google.maps.LatLng\((\d*\.?\d*), (\d*\.?\d*)\)')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1] 
        
    
        description = response.css('.project-info-block .project-info-box p::text').get()
        description = remove_white_spaces(description)
        
        images = response.css('.fotorama img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        
        floor = response.css(".p-info-list li .p-label:contains('Piano') + .p-value::text").get()
        if floor:
            floor = remove_white_spaces(floor)

        elevator = response.css(".p-info-list li .p-label:contains('Ascensore') + .p-value::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False

        
        landlord_email = "immobiliare@bortolai.it"
        landlord_phone = "+39010279900"
        landlord_name = "Immobiliare Bortolai.it S.r.l."
                
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
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
