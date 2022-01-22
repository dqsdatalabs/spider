from requests.api import post
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

class Consul_capital_Spider(scrapy.Spider):

    name = 'consulcapital'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['consulcapital.it']
    start_urls = ['https://www.consulcapital.it/immobili/?status=affitto']

    position = 1

    


    def parse(self, response):
        pages = response.css('.rh_pagination_classic a::attr(href)').getall()
        for page in pages:
            yield Request(page,callback=self.parseApartment)

        
    
    def parseApartment(self,response):
        apartments = response.css('.property-status-affitto')
        
        for apartment in apartments:
            isApartment = str(apartment.css('h5 small::text').get())
            if 'Appartamento' in isApartment:
                rentTxt = apartment.css('.price::text').get().replace('€',"").replace('.',"")
                rent = int(rentTxt)
                meta = {
                    "rent":rent
                }
                url = apartment.css('h4 a::attr(href)').get()
                yield Request(url,callback=self.parseDetails,meta=meta)

    def parseDetails(self,response):
        print('='*50)
        print(response.url)
        print('='*50)
        
        title = response.css('.wrap.clearfix h1 *::text').get()

        #description
        description = response.css('.content.clearfix p::text').get()

        infoText = response.css('.property-meta.clearfix *::text').getall()
        info = remove_white_spaces("-".join(infoText))
        info = info.replace('\n',"")

        rex = re.findall(r'G\d+|. Camere|. Bagni|\d+',info)
        room_count=1
        bathroom_count=1
        square_meters = int(rex[1])
        external_id = rex[0]
        for grp in rex:
            if 'Camere' in grp:
                room_count=abs(int(grp[0]))
            if 'Bagni' in grp:
                bathroom_count=abs(int(grp[0]))



     
        
        
        rent = response.meta['rent']
        
        images = response.css('.slides a::attr(href)').getall()
        

        property_type = 'apartment'

 
        if rent>0 and 'Ufficio' not in title:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            
            item_loader.add_value("description", description)
            #item_loader.add_value("city", city)
            #item_loader.add_value("zipcode", zipcode)
            #item_loader.add_value("address", address)
            #item_loader.add_value("latitude", latitude)
            #item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            #item_loader.add_value("energy_label", energy_label)
            #item_loader.add_value("furnished", furnished)
            #item_loader.add_value("floor", floor)
            #item_loader.add_value("parking", parking)
            #item_loader.add_value("elevator", elevator)
            #item_loader.add_value("balcony", balcony)
            
            #item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", 'consul_capital')
            #item_loader.add_value("landlord_email", 'immobiliare@eynard-immobiliare.it')
            item_loader.add_value("landlord_phone", '+39 351 928 3067')
            item_loader.add_value("position", self.position)

            self.position+=1

            
            yield item_loader.load_item()