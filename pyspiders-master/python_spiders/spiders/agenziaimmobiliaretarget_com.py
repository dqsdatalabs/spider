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

class AgenziaimmobiliaretargetSpider(scrapy.Spider):
        
    name = 'agenziaimmobiliaretarget'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['agenziaimmobiliaretarget.com']
    post_url = 'https://agenziaimmobiliaretarget.com/annunci-immobiliari'
    
    position = 1

    formdata = {
        "filter_location": "",
        "filter_property_type": "1001",
        "filter_contract": "1",
        "size_min": "",
        "price_min": "",
        "price_max": "",
        "code": ""
    }
    
    def start_requests(self):
        yield FormRequest(
                        url = self.post_url, 
                        formdata = self.formdata, 
                        callback = self.parse, 
                        dont_filter = True
                        )


    def parse(self, response):

            
        cards = response.css(".col-md-6 > .listing-box")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css("h2 a::attr(href)").get()
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            AgenziaimmobiliaretargetSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        
        nextPageUrl = response.css("li.page-item.active + li.page-item a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("strong:contains('Riferimento') + span::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)




        square_meters = response.css("strong:contains('MQ') + span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
            

        room_count = response.css("strong:contains('Vani') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".amenities li:contains('Bagn')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        
        rent = response.css("strong:contains('Prezzo') + span::text").get()
        if "Trattativa Riservata" not in rent:
            if rent:
                rent = extract_number_only(rent).replace(".","")
        else:
            rent = None
            
        currency = response.css("strong:contains('Prezzo') + span::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".content-title h1::text").get()
        if title:
            title = remove_white_spaces(title)   
            

        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r'var myLatLng = {lat: (\d*\.?\d*), lng: (\d*\.?\d*)};')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
            
        
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        
        zipcode = address = city = None
        if "address" in responseGeocodeData:
            zipcode = responseGeocodeData['address']['Postal']
            address = responseGeocodeData['address']['LongLabel']
            city = responseGeocodeData['address']['City']
            
        if not latitude:
            address = city = title
    
        description = response.css("h2:contains('Descrizione') + p::text").get()
        description = remove_white_spaces(description)
        
        
        images = response.css('.gallery img.gallery-item::attr(src)').getall()
        external_images_count = len(images)

        energy_label = response.css(".amenities li:contains('Classe energetica')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split("Classe energetica ")[1]
        
        floor = response.css(".amenities li:contains('Pian')::text").get()
        if floor:
            floor = remove_white_spaces(floor).split(": ")[1]

        elevator = response.css(".amenities li:contains('Ascensore')::attr(class)").get()
        if elevator == "yes":
            elevator = True
        else:
            elevator = False  
            
        balcony = response.css(".amenities li:contains('Balcone')::attr(class)").get()
        if balcony == "yes":
            balcony = True
        else:
            balcony = False  
 
        swimming_pool = response.css(".amenities li:contains('Piscina')::attr(class)").get()
        if swimming_pool == "yes":
            swimming_pool = True
        else:
            swimming_pool = False  

        parking = response.css(".amenities li:contains('Posto Auto')::attr(class),.amenities li:contains('Posti auto')::attr(class)").get()
        if parking == "yes":
            parking = True
        else:
            parking = False  
            
            
        furnished = response.css(".amenities li:contains('Arredato')::attr(class)").get()
        if furnished == "yes":
            furnished = True
        else:
            furnished = False  
            
            
        
        washing_machine = response.css(".amenities li:contains('Lavatrice')::attr(class)").get()
        if washing_machine == "yes":
            washing_machine = True
        else:
            washing_machine = False 
        
        dishwasher = response.css(".amenities li:contains('Lavastoviglie')::attr(class)").get()
        if dishwasher == "yes":
            dishwasher = True
        else:
            dishwasher = False 
        
        
        landlord_phone = "+390108376598"
        landlord_email = "info@targetimmobiliare.net"
        landlord_name = "L’Agenzia Immobiliare Target"



        if rent: 
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
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
