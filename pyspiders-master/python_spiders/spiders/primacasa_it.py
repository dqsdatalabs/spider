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

class PrimacasaSpider(scrapy.Spider):
        
    name = 'primacasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.primacasa.it']
    post_url = 'https://www.primacasa.it/it/i/immobili.html'
    
    position = 1

    formdata = {
        "pagina": "1",
        "ordinamento": "prezzo",
        "direzione": "ASC",
        "id_categoria": "1",
        "id_contratto": "2"
    }
    
    def start_requests(self):
        yield FormRequest(
                        url = self.post_url, 
                        formdata = self.formdata, 
                        callback = self.parse, 
                        dont_filter = True
                        )


    def parse(self, response):
        
        
        cards = response.css(".properties-grid .property-grid-item")
        

        for index, card in enumerate(cards):

            card_type = card.css(".item-badge.bg-yellow.verde::text").get()
            if "vendita" in card_type:
                continue

            
            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css("h2.item-title a::attr(href)").get()
            # if card_url:
            #     card_url = response.urljoin(card_url)
                

            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            PrimacasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0 :
            
            nextPageUrlNumber = int(response.css("li.active a::attr(pagina)").get()) + 1
            lastPageUrlNumber = int(response.css("li.last a::attr(pagina)").get())
            prevPageUrlNumber = self.formdata['pagina']
            
            self.formdata['pagina'] = str(nextPageUrlNumber)
            if nextPageUrlNumber <= lastPageUrlNumber:
                yield FormRequest(url = self.post_url, callback = self.parse, formdata=self.formdata, dont_filter=True)


    def parseApartment(self, response):

        external_id= None
        square_meters= None
        room_count= None
        square_meters= None
        
        external_id = response.css(".core-details div.col-6:contains('Riferimento') + div::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css(".section-title li:contains('MQ')::text").getall()
        square_meters = " ".join(square_meters)
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".section-title li:contains('Camere')::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
            if room_count == 0:
                room_count = 1
        else:
            room_count = 1

        bathroom_count = response.css(".section-title li:contains('Bagni')::text").get()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
            if bathroom_count == 0:
                bathroom_count = 1       
        else:
            bathroom_count = 1

        rent = response.css(".price-text.verde::text").get()
        if rent and "Trattativa riservata" not in rent:
            rent = rent.split(",")[0]
            rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css(".price-text.verde::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css("h2.big-title::text").get()
        if title:
            title = remove_white_spaces(title)
        
                        
        city = response.css(".small-title::text").get()
        if city:
            city = remove_white_spaces(city)
        
        address = f"{title} - {city}"
            
        script_map = response.css("p + iframe::attr(src)").get()
        if script_map:
            pattern = re.compile(r'maps.google.com\/maps\?q=(\d*\.?\d*),(\d*\.?\d*)&')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        else:
            latitude = None
            longitude = None
    
        description = response.css('.details-image + div p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.carousel-inner:nth-child(2) .carousel-item  a::attr(href)').getall()
        external_images_count = len(images)
        

        energy_label = response.css(".badge-classe-energetica::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        elevator = response.css(".area-title.verde:contains('Accessori') + div:contains('Ascensore') i::attr(class)").get()
        if "check" in elevator:
            elevator = True
        else:
            elevator = False
        
        washing_machine = response.css(".area-title.verde:contains('Accessori') + div:contains('Lavanderia') i::attr(class)").get()
        if "check" in washing_machine:
            washing_machine = True
        else:
            washing_machine = False
            
        swimming_pool = response.css(".area-title.verde:contains('Accessori') + div:contains('Piscina') i::attr(class)").get()
        if "check" in swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False 
          
        
        landlord_name = response.css(".title-link.verde::text").getall()
        landlord_name = " ".join(landlord_name)
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
            
        landlord_phone = response.css(".property-content .click_phone::text").getall()
        landlord_phone = " ".join(landlord_phone)
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)
            
        landlord_email = response.css(".property-content .click_phone + a::text").getall()
        landlord_email = " ".join(landlord_email)
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
                
        
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
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
