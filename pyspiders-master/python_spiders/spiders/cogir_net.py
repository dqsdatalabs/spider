from typing import Text
import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from python_spiders.spiders import truepennys_com

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json

class CogirSpider(scrapy.Spider):


    name = 'cogir'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f'{name.capitalize()}_PySpider_{country}_{locale}'
    allowed_domains = ['cogir.net']
    start_urls = ['https://www.cogir.net/en/residential-buildings.html?province=&budget=0&nbPiece=']

    position = 1

    def parse(self, response):
        prop_urls=response.css('.propItem a::attr(href)').getall()
        for prop in prop_urls:
            url = 'https://www.cogir.net/'+prop
            yield Request(url,
                 callback=self.parseApartment)


    def parseApartment(self, response):
        
        
        title=response.css('.TitreAdd h1::text').get()


        self.description = remove_white_spaces("".join(response.css('#description .incTinyMce *::text').getall()))

        address = remove_white_spaces(response.css('.TitreAdd p::text').get())

        images = set(response.css('#PhotoPrinc::attr(src)').getall())
        images = ['https://www.cogir.net/'+x for x in images]


        script_map = response.css('#carteLogos > script::text').get()
        if script_map:
            script_map = remove_white_spaces(script_map)
            x = re.findall("-?\d+\.\d+", script_map)
            self.latitude = x[0]
            self.longitude = x[1]

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={self.longitude},{self.latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        self.zipcode = responseGeocodeData['address']['Postal']
        self.city = responseGeocodeData['address']['City']
            
        
        rooms = response.css("#tableModele tr:contains('$') .colType::text").getall()

        bathrooms = response.css("#tableModele tr:contains('$') .colModele:contains('athroom')::text").getall()
        
        rents = response.css("#tableModele tr:contains('$') .colPrix::text").getall()
    
        

        room_count=[]
        bathroom_count=[]
        rent=[]
        property_type = []
        
        i=0
        while(True):
            if i< len(rents) and'$' in rents[i].lower():

                count=''
                for c in rents[i]:
                    if c.isdigit():
                        count+=c
                if len(count)==0:
                    break
                rent.append(int(count))
            else:
                break

            if 'bachelor' in rooms[i].lower():
                property_type.append('studio')
            else:
                property_type.append('apartment')
            
            if i<len(rooms) and 'bedroom' in rooms[i].lower():
                count = [int(s) for s in rooms[i].split() if s.isdigit()]
                if len(count)==0:
                    count.append(1)
                room_count.append(count[0])
            else:
                room_count.append(1)
            
            if i< len(bathrooms) and 'bathroom' in bathrooms[i].lower():
                count = [int(s) for s in bathrooms[i].split() if s.isdigit()]
                if len(count)==0:
                    count.append(1)
                bathroom_count.append(count[0])
            else:
                bathroom_count.append(1)
            

            i+=1

        contactInfo = self.getContactInfo(response)
        self.landlord_name=''
        self.landlord_phone=''
        self.landlord_email=''
        if len(contactInfo)>0:
            self.landlord_name= remove_white_spaces(contactInfo[0].replace('-',''))
            if len(contactInfo)>1:
                self.landlord_phone=contactInfo[1]

        for idx in range(0,len(rent)):
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url+'#{v}'.format(v=idx))
            item_loader.add_value("external_source", self.external_source)
            #item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", self.description)
            item_loader.add_value("city", self.city)
            item_loader.add_value("zipcode", self.zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", self.latitude)
            item_loader.add_value("longitude", self.longitude)
            item_loader.add_value("property_type", property_type[idx])
            #item_loader.add_value("square_meters", int(int(square_meters)*10.764))

            item_loader.add_value("room_count", room_count[idx])
            item_loader.add_value("bathroom_count", bathroom_count[idx])
            item_loader.add_value("rent", rent[idx])

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            
            item_loader.add_value("currency", "CAD")
            item_loader.add_value("balcony", True if 'alcon' in self.description else False)
            item_loader.add_value('washing_machine',True if 'aundry' in self.description else False)
            item_loader.add_value('dishwasher',True if 'washer' in self.description else False)
            item_loader.add_value('parking',True if 'parking' in self.description else False)
            item_loader.add_value('elevator',True if 'levator' in self.description else False)
            item_loader.add_value("terrace", True if 'Terrace' in self.description else False)

            if self.landlord_name!='':
                item_loader.add_value("landlord_name", self.landlord_name)
            item_loader.add_value("landlord_email",'info@cogir.net')
            if self.landlord_phone!='':
                item_loader.add_value("landlord_phone", self.landlord_phone)

            item_loader.add_value("position", self.position)
            self.position+=1

            yield item_loader.load_item()

            #print(complete_url_next_page)
            

    def getContactInfo(self, response):
        contactInfo = "".join(response.css("#blocContact *:contains('-')::Text").getall())
        x = contactInfo.find('-')
        info =[]
        info.append(remove_white_spaces(contactInfo[:x]))
        info.append(remove_white_spaces(contactInfo[x+1:]))
        return info
        