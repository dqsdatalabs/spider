from operator import le
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
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
import math

class Firststaybc_Spider(scrapy.Spider):

    name = 'firststaybc'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['firststaybc.com']
    start_urls = ['https://firststaybc.com/offer-type/rent/']

    position = 1


    def parse(self, response):

        myhome = response.css('#myhome-min-js-extra').get()
        rex1 = re.findall(r'MyHomeListing\d+ .+MyHomeListing\d+"}',str(myhome))
        rex = re.findall(r'MyHomeListing\d+ = ',rex1[0])
        stringJson = rex1[0].replace(rex[0],"")
        result = json.loads(stringJson)
        apartments = result['results']['estates']

        
        for apartment in apartments:
            external_id = str(apartment['id'])
            external_link = apartment['link']
            title = apartment['name']
            
            address = apartment['address']

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']

            longitude  = str(longitude)
            latitude  = str(latitude)

            try:
                rent = int(re.findall('\d+.\d+',apartment['price'][0]['price'])[0].replace('.',''))
            except:
                rent = 0

            attributes = apartment['attributes']
            property_type = '' if attributes[0]['values'][0]['name']=='Office' else 'apartment'
            elevator = attributes[4]['has_archive']
            '''images = []
            pics = apartment['gallery']
            for image in pics:
                images.append(image['image'])
            images = [re.sub(r'-\d*x\d*', "", img) for img in images]'''
            if property_type!='':
                room_count = int(attributes[5]['values'][0]['name'])
                bathroom_count = math.floor(float(attributes[6]['values'][0]['name']))
                
                try:
                    square_meters = int(float(attributes[7]['values'][0]['value'])/10.764)

                except:
                    square_meters=0

                try:
                    furnished = True if attributes[11]['values'][0]['name'] =='Yes' else False
                except:
                    furnished=False


                dataUsage = {
                    "property_type": property_type,
                    'title':title,
                    "external_id": external_id,
                    "external_link": external_link,
                    "city": city,
                    "address":address,
                    "zipcode":zipcode,
                    "furnished":furnished,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "elevator": elevator,
                    "rent": rent
                    }
    
            


                yield Request(external_link, meta=dataUsage,
                    callback=self.parseApartment)
        
    def parseApartment(self,response):
        
        description = "".join(response.css('.mh-estate__section.mh-estate__section--description *::text').getall())
        description = remove_white_spaces(description)

        images = response.css('.swiper-slide a::attr(href)').getall()
        if response.css(".mh-estate__list .mh-estate__list__inner .mh-estate__list__element.mh-estate__list__element--dot:contains('Dishwasher')"):
            dishwasher=True
        else:
            dishwasher=False

        if response.css(".mh-estate__list .mh-estate__list__inner .mh-estate__list__element.mh-estate__list__element--dot:contains('Swimming Pool')"):
            swimming_pool=True
        else:
            swimming_pool=False

        if response.css(".mh-estate__list .mh-estate__list__inner .mh-estate__list__element.mh-estate__list__element--dot:contains('Parking')"):
            parking=True
        else:
            parking=False
        
        if response.css(".mh-estate__list .mh-estate__list__inner .mh-estate__list__element.mh-estate__list__element--dot:contains('Laundry')"):
            washing_machine=True
        else:
            washing_machine=False

        if response.meta['elevator']==True or response.css(".mh-estate__list .mh-estate__list__inner .mh-estate__list__element.mh-estate__list__element--dot:contains('Elevator')"):
            elevator=True
        else:
            elevator=False
 
        if response.meta['rent']>0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta["external_id"])
            item_loader.add_value("title", response.meta["title"])
            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", int(int(response.meta['square_meters'])*10.764))
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", "CAD")
            #item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", response.meta['furnished'])
            #item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("parking", parking)
            #item_loader.add_value("balcony", response.meta['balcony'])
            #item_loader.add_value("terrace", response.meta['terrace'])
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'Natalia Garbuzova')
            item_loader.add_value("landlord_email", 'info@firststaybc.com')
            item_loader.add_value("landlord_phone", '778.317.6393')
            item_loader.add_value("position", self.position)

            self.position+=1
            yield item_loader.load_item()