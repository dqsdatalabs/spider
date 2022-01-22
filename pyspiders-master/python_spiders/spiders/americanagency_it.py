from functools import partial
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


class kwSpider(scrapy.Spider):

    name = 'americanagency'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1
    def start_requests(self):
        start_url = 'https://americanagency.it/ita/immobiliLocazione.asp'
        yield FormRequest(start_url,formdata={'tipologiaImmobile': '10'},method='POST',callback=self.parse)
    

    def parse(self, response):

        apartments = response.css('.mbr-figure.col-md-6 a::attr(href)').getall()
        apartments = ['https://americanagency.it/ita/' +x for x in apartments]
        for apartment in apartments:
            yield Request(apartment,callback=self.parseApartment)

    def parseApartment(self, response):
        external_link = response.url
        external_id = external_link.split('id=')[-1]

        title = remove_white_spaces("".join(response.css('h3.align-center strong *::text').getall()))
        rent = int(response.css('.title h3::text').get().split('€')[-1].replace(' ',''))
        text = response.css('.container .media-container-row .mbr-text.col-12:nth-child(1) *::text').getall()
        description = remove_white_spaces("".join([x for x in text if 'Per gli immobili pubblicizzati non ancora' not in x]))
        images = response.css('.carousel-item img::attr(src)').getall()
        images = ['https://americanagency.it'+x for x in images]
        rex = re.findall(r'-?\d+\.\d+',"".join(response.css("script[type='text/javascript']").getall()))
        zipcode=''
        city=''
        address=''
        longitude=''
        latitude=''
        if rex:
            latitude=rex[0]
            longitude=rex[1]
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
            longitude  = str(longitude)
            latitude  = str(latitude)

        room_count = 1
        if 'VANI' in response.css('.title h3::text').get():
            rex = re.search(r'\d+',response.css('.title h3::text').get())
            if rex and int(rex[0])<15:
                room_count=int(rex[0])

        property_type = 'apartment' if 'appartamento' in description else 'house'
        currency = 'EUR'

        washing_machine = 'lavanderia' in description
        parking = 'posti auto' in description
        furnished = 'arredato' in description
        swimming_pool = 'piscina' in description
        terrace = 'terrazz' in description
        balcony = 'balcon' in description

        rex = re.search(r'(\d+) mq|(\d+) Mq|(\d+) MQ|(\d+) mQ|mq (\d+)',description)
        square_meters=''
        if rex:
            square_meters=re.search(r'\d+',rex[0])[0]

        dataUsage = {
            "property_type": property_type,
            "title": title,
            "external_id": external_id,
            "external_link": external_link,
            "city": city,
            "address": address,
            "zipcode": zipcode,
            "longitude": longitude,
            "latitude": latitude,
            "washing_machine": washing_machine,
            "furnished": furnished,
            "swimming_pool": swimming_pool,
            "terrace": terrace,
            "balcony": balcony,
            "square_meters": square_meters,
            "room_count": room_count,
            "images": images,
            #"bathroom_count": bathroom_count,
            "parking": parking,
            "rent": rent,
            "currency": currency,
            'description':description,
        }
        yield Request(response.url,meta=dataUsage,dont_filter=True,callback=self.save)
       

    def save(self, response):

        property_type     =response.meta["property_type"]    
        title             =response.meta['title']   
        external_id       =response.meta["external_id"]      
        #available_date    =response.meta['available_date']   
        #external_link     =response.meta["external_link"]    
        city              =response.meta["city"]             
        address           =response.meta["address"]          
        zipcode           =response.meta["zipcode"]      
        furnished         =response.meta["furnished"]        
        longitude         =response.meta["longitude"]    
        latitude          =response.meta["latitude"]     
        square_meters     =response.meta["square_meters"]    
        room_count        =response.meta["room_count"]   
        #bathroom_count    =response.meta["bathroom_count"]   
        #landlord_phone    =response.meta['landlord_phone']   
        #landlord_email    =response.meta['landlord_email']   
        washing_machine   =response.meta["washing_machine"]  
        balcony           =response.meta["balcony"]          
        #dishwasher        =response.meta["dishwasher"]       
        terrace        =response.meta["terrace"]       
        #floor_plan_images =response.meta['floor_plan_images'] 
        images            =response.meta["images"]           
        rent              =response.meta["rent"]  
        parking              =response.meta["parking"]  
        currency          =response.meta["currency"]  
        swimming_pool     =response.meta["swimming_pool"]  
        description       =response.meta["description"]  

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)

        item_loader.add_value("description", description)
        item_loader.add_value("city",  city)
        item_loader.add_value("zipcode",  zipcode )
        item_loader.add_value("address",  address )
        item_loader.add_value("latitude",  latitude )
        item_loader.add_value("longitude",  longitude )

        item_loader.add_value("property_type",  property_type )
        item_loader.add_value("square_meters",  square_meters )
        item_loader.add_value("room_count", room_count)
        #item_loader.add_value("bathroom_count",  bathroom_count )

        item_loader.add_value("images",  images )
        item_loader.add_value("external_images_count",
                            len(images))

        item_loader.add_value("rent",  rent )
        item_loader.add_value("currency",  currency )

        #item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("furnished", furnished)
        #item_loader.add_value("floor", floor)
        item_loader.add_value("parking",  parking )

        #item_loader.add_value("elevator", elevator)
        item_loader.add_value('washing_machine', washing_machine)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value('swimming_pool', swimming_pool)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", 'americanagency')
        item_loader.add_value("landlord_email", 'am@americanagency.it')
        item_loader.add_value("landlord_phone", '+39 055 475 053')
        item_loader.add_value("position", self.position)
        self.position+=1
        yield item_loader.load_item()
