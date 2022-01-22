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

class AbytareSpider(scrapy.Spider):
        
    name = 'abytare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.abytare.com']
    start_urls = ['https://abytare.com/it/annunci-immobiliari/?pag=1&tipo_contratto=&tipo_annuncio=2&tipo_immobile=0&id_regione=0&id_provincia=&id_comune=&ord=1&id_agenzia=&tipologia2=residenziale']

    position = 1
    def parse(self, response):
            
        cards = response.css(".contentAnnunci .elencoann")

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".anteprimaann a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)
                  
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            AbytareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
            
        nextPageUrl = response.css("#Paginatore .PaginatoreSel + .PaginatoreLink  a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True)


    def parseApartment(self, response):

        property_type = "apartment"
            
        external_id = response.css("#side::text").getall()
        external_id = list(filter(lambda x: "Codice" in x,  external_id))
        external_id = " ".join(external_id)
        if external_id:
            external_id = remove_white_spaces(external_id).split(": ")[1]

        square_meters = response.css("#side strong:contains('mq')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".schedaTecnica div:contains('Vani')::text").getall()
        room_count = " ".join(room_count)
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".schedaTecnica div:contains('Bagni')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        else:
            bathroom_count = 1         

        rent = response.css("#side strong:contains('€')::text").get()
        if rent:
            rent = extract_number_only(rent).split(".")[0]
        else:
            rent = None
            
        currency = response.css("#side strong:contains('€')::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".giallo + h1::text").get()
        if title:
            title = remove_white_spaces(title)   
            
        city = response.xpath("//*[@id='side']/text()[2]").get()
        if city:
            city = remove_white_spaces(city).split(": ")[1]
            city = city.split(", ")[0]
            
        street = response.xpath("//*[@id='side']/text()[3]").get()
        if street:
            street = remove_white_spaces(street)
        
        address = f"{street} - {city}"            
        
        zipcode = response.css("#showmap2 script::text").get()
        zipcode = remove_white_spaces(zipcode)
        if zipcode:
            pattern = re.compile(r'showAddress\("([a-zA-Z0-9_\ ]*), ([\d\ ]*) (\w*)"\);')
            x = pattern.search(zipcode)
            zipcode = x.groups()[1]
            if " " in zipcode:
                zipcode = zipcode.split(" ")[1]
            else:
                zipcode = zipcode
        
    
        description = response.css(".giallo + h1 +h2::text").getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.frgallery img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        
        energy_label = response.css(".schedaTecnica div:contains('Classe:')::text").getall()
        energy_label = " ".join(energy_label)
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split("Classe:")[1].strip()
        
        floor = response.css(".schedaTecnica div:contains('Piano')::text").getall()
        floor = " ".join(floor)
        if floor:
            floor = remove_white_spaces(floor).split("Piano:")[1].strip()
        
            

        
        balcony = response.css(".schedaTecnica div:contains('Balconi:')::text").getall()
        if balcony:
            balcony = True
        else:
            balcony = False  
        
              
        furnished = response.css(".schedaTecnica div:contains('Arredi:')::text").getall()
        if "Non Arredato" in furnished:
            furnished = False
        elif furnished:
            furnished = True
        else:
            furnished = False  
                
        
        terrace = response.css(".schedaTecnica div:contains('Terrazzi:')::text").getall()
        if terrace:
            terrace = True
        else:
            terrace = False  
        
        
        landlord_email = response.css("p.giallo a[title*='Mail']::text").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email) 
            
        landlord_phone = response.css("p.giallo a[title*='Telefono']::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone) 
            
        landlord_name = response.xpath("/html/body/div[1]/div[3]/div[2]/div[2]/text()[3]").get()
        landlord_name += response.xpath("/html/body/div[1]/div[3]/div[2]/div[2]/em/text()").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name) 


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
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
