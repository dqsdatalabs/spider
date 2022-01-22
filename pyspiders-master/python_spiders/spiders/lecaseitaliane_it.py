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

class LecaseitalianeSpider(scrapy.Spider):
        
    name = 'lecaseitaliane'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.lecaseitaliane.it']
    start_urls = ['https://www.lecaseitaliane.it/immobili/affitti-milano/']

    position = 1

    def parse(self, response):
        
        
        
        cards = response.css(".immobile")
        

        for index, card in enumerate(cards):

            position = self.position
            
            property_type = "apartment"
            
            card_url = card.css(".block-info a::attr(href)").get()
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }
            
            
            LecaseitalianeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        

    def parseApartment(self, response):


        external_id = response.css(".info-immobile p span:contains('Riferimento')::text").get()
        if external_id:
            external_id = external_id.split("Riferimento ")[1]
        
        square_meters = response.css(".caratt-immob li:contains('MQ')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".caratt-immob li:contains('Camera')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)

        bathroom_count = response.css(".caratt-immob li:contains('Bagno')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)           

        rent = response.css("h3:contains('€')::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")
            
        currency = "EUR"
        
        title = response.css("header h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        
        address = response.css(".info-immobile p::text").get()
        if address:
            address = remove_white_spaces(address)
                
        script_map = response.css("#map-google::attr(src)").get()
        if script_map:
            pattern = re.compile(r'maps.google.com\/maps\?q=(\d*\.?\d*),(\d*\.?\d*)&')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1] 
        
        
        if latitude != "0":
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']    
        else:
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            longitude = str(longitude)
            latitude = str(latitude)
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']  
        
        description = response.css('.entry-content p::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('img.img-responsive::attr(data-src)').getall()
        external_images_count = len(images)

        energy_label = response.css("h3:contains('Energetica') + ul li:contains('Classe')::text").get()
        if energy_label:
            energy_label = energy_label.split(" ")[1]
        
        floor = response.css(".caratt-immob li:contains('Piano')::text").get()
        if floor:
            floor = floor.split(": ")[1]

        elevator = response.css(".caratt-immob li:contains('Ascensore')::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False
         
        washing_machine = response.css(".caratt-immob li:contains('Lavatrice')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False
             
            
        balcony = response.css(".caratt-immob li:contains('Balcone')::attr(class)").get()
        if balcony != "negative":
            balcony = True
        elif balcony == "negative":
            balcony = False
        else:
            balcony = False   

        terrace = response.css(".caratt-immob li:contains('Terrazzo')::attr(class)").get()
        if terrace != "negative":
            terrace = True
        elif terrace == "negative":
            terrace = False
        else:
            terrace = False    
        
        utilities = response.css(".caratt-immob li:contains('Spese')::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)

        furnished = response.css(".caratt-immob li:contains('Arredato')::attr(class)").get()
        if furnished != "negative":
                furnished = True
        elif furnished == "negative":
            furnished = False
        else:
            furnished = False  
    

        
        landlord_email = "info@caseitaliane.it"
        landlord_phone = "+393337486559"
        landlord_name = "Case Italiane Immobiliare"
    
        

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
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
