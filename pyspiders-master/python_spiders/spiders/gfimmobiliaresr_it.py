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


class Gfimmobiliaresr_Spider(scrapy.Spider):

    name = 'gfimmobiliaresr'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1
    def start_requests(self):
        for i in range(1,5):
            start_url = f'https://gfimmobiliaresr.it/property-search/page/{i}/?status=affitto&type=appartamento'
            yield Request(start_url,callback=self.parse)


    def parse(self, response):
        print(response.url)
        
        apartments = response.css('.span6 .detail')

        for apartment in apartments:
            rent = extract_number_only(apartment.css('.price::text').get().replace(',00','').replace('.',''))
            if 'ommerciali' in apartment.css('.price small::text').get() or 'fficio' in apartment.css('.price small::text').get():
                continue
            url = apartment.css('a::attr(href)').get()
            yield Request(url,meta={'rent':rent},dont_filter=True,callback=self.parseApartment)

    def parseApartment(self, response):
        rent = response.meta['rent']
        external_link = response.url
        address = remove_white_spaces(response.css('address.title::text').get())

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        #address = responseGeocodeData['address']['Match_addr']
        longitude  = str(longitude)
        latitude  = str(latitude)


        title = remove_white_spaces(response.css('h1.page-title *::text').get())
        description = remove_white_spaces("".join(response.css('.content.clearfix *::text').getall()))
        
        images = response.css('.slides li a::attr(href)').getall()
        square_meters=0
        if response.css(".property-meta.clearfix span:contains('mq')"):
            square_meters = re.search(r'\d+',response.css(".property-meta.clearfix span:contains('mq')::text").get())[0]
        if response.css(".property-meta.clearfix span:contains('MQ')"):
            square_meters = re.search(r'\d+',response.css(".property-meta.clearfix span:contains('MQ')::text").get())[0]
        external_id=''
        try:
            external_id = remove_white_spaces(response.css(".property-meta.clearfix span:contains('A')::text").get())
        except:
            external_id=''

        room_count=1
        try:
            room_count = int(re.search(r'\d+',response.css(".property-meta.clearfix span:contains('Camere')::text").get())[0])
        except:
            room_count = 1

        bathroom_count=1
        try:
            bathroom_count = int(re.search(r'\d+',response.css(".property-meta.clearfix span:contains('Bathroom')::text").get())[0])
        except:
            bathroom_count = 1


        property_type = 'apartment' if 'appartamento' in description or 'APPARTAMENTO' in title else 'house'
        currency = 'EUR'

        washing_machine = 'lavanderia' in description
        parking = 'posti auto' in description
        furnished = 'arredato' in description
        swimming_pool = 'piscina' in description
        terrace = 'terrazz' in description
        balcony = 'balcon' in description

        landlord_phone = remove_white_spaces("".join(response.css(".mobile:contains(Mobile)::text").getall()).replace('Mobile : ',''))
        if not landlord_phone or len(landlord_phone)<5:
            landlord_phone='0931 68004'
        landlord_name = response.css('.left-box h3::text').get()
        if not landlord_name or len(landlord_name)<7:
            landlord_name='GF Immobiliare'

        landlord_email = 'info@gfimmobiliaresr.it'


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
            "bathroom_count": bathroom_count,
            "parking": parking,
            "rent": rent,
            "currency": currency,
            'landlord_phone':landlord_phone,
            'landlord_name':landlord_name,
            'landlord_email':landlord_email,
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
        bathroom_count    =response.meta["bathroom_count"]   
        landlord_phone    =response.meta['landlord_phone']   
        landlord_email    =response.meta['landlord_email']   
        landlord_name    =response.meta['landlord_name']   
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

        if int(rent)>0 and int(rent)<1600:
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
            item_loader.add_value("bathroom_count",  bathroom_count )

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
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", self.position)
            self.position+=1
            yield item_loader.load_item()