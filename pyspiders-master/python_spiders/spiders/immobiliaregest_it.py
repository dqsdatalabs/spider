from requests.api import post
from requests.models import Response
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

class Immobiliaregest_Spider(scrapy.Spider):

    name = 'immobiliaregest'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['immobiliaregest.it']
    start_urls = ['https://www.immobiliaregest.it/proprieta/?status=affitto']

    position = 1


    def parse(self, response):
        pages = response.css('.pagination a::attr(href)').getall()

        for page in pages:
            yield Request(page,callback=self.parseApartment)

        
    
    def parseApartment(self,response):
        apartments = response.css('.property-item.clearfix')
        
        for apartment in apartments:
            
            title = apartment.css('h4 *::text').get()
            meta = {
       
                'title':title
            }
            url = apartment.css('a::attr(href)').get()
            yield Request(url,callback=self.parseDetails,meta=meta)

    def parseDetails(self,response):
        print('='*50)
        print(response.url)
        print('='*50)
        
        try:
            external_id = response.css(".title::text").get().split(' : ')[1]
        except:
            external_id = ''

        #description
        descriptionText = response.css('.content.clearfix *::text').getall()
        description = " ".join(descriptionText)
        description =  remove_white_spaces(description)

        #Rent
        rentText = "".join(response.css("h5.price *::text").getall()).replace('\n',"")
  
        if 'ppartamento' in rentText or 'Camera' in rentText or 'ppartamento' in description:
            property_type="apartment" 
        else:
            property_type='house' 

        if 'ppartamento' in rentText or 'Camera' in rentText or 'Baita' in rentText or 'ppartamento' in description or 'camera' in description:
            if 'Vendita' in rentText or 'Box' in rentText:
                return
            rex = re.findall(r'\d+.\d+',"".join(rentText))
            if rex:
                rent = int(rex[0].replace('.',""))
     
            else:
                try:
                    rentText = response.css('.content.clearfix h6 *::text').getall()
                    rex = re.findall(r'\d+.\d+',"".join(rentText))
                    rent = int(rex[0].replace('.',""))

                except:
                    return
        else:
            rent=0

        square_metersText = response.css('.property-meta.clearfix span::text').get()
        if square_metersText:
            digits = [int(s) for s in square_metersText.split() if s.isdigit()]
            if len(digits)>0:
                square_meters=int(digits[0])
            else:
                square_meters=0
        else:
            square_meters = 0
        if square_meters<10:
            square_meters = 0
        print(square_meters)
        bedroomText = response.css(".property-meta.clearfix span:contains('amer')::text").get()
        if bedroomText:
            room_count = re.findall(r'\d+',bedroomText)[0]
        else:
            room_count=1

        bathroomText = response.css(".property-meta.clearfix span:contains('agn')::text").get()
        if bathroomText:
            bathroom_count = re.findall(r'\d+',bathroomText)[0]
        else:
            bathroom_count=1

        parkingText = response.css(".property-meta.clearfix span:contains('Garage')::text").get()
        if parkingText:
            parking = True
        else:
            parking = False

        if response.css(".features *:contains('BALCONE')::text").get():
            balcony = True
        else:
            balcony = False

        if response.css(".features *:contains('LAVATRICE')::text").get():
            washing_machine = True
        else:
            washing_machine = False

        if response.css(".features *:contains('ENSOR')::text").getall():
            elevator = True
        else:
            elevator = False

        images = response.css(".slides a::attr(href)").getall()

        map = response.css(".map-wrap.clearfix script:contains('lang')").get()
        zipcode=''
        city=''
        address=''
        latitude=''
        longitude=''
        if map:
            rex = re.findall(r'"\d+.\d+"',map)
            if rex:
                latitude = rex[0].replace("\"","")
                longitude = rex[1].replace("\"","")

                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()

                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']


  
        
        if rent>0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", response.meta['title'])
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
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("washing_machine",washing_machine)
            item_loader.add_value("currency", "EUR")
            #item_loader.add_value("energy_label", energy_label)
            #item_loader.add_value("furnished", furnished)
            #item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            
            #item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", 'immobiliaregest')
            item_loader.add_value("landlord_email", ' info@immobiliaregest.it')
            item_loader.add_value("landlord_phone", '+393357597530')
            item_loader.add_value("position", self.position)

            self.position+=1

            
            yield item_loader.load_item()