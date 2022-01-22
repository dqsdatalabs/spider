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

class Albertamanagement_Spider(scrapy.Spider):

    name = 'albertamanagement'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['albertamanagement.com']

    position = 1

    stopParsing = False
    def start_requests(self):

        start_url = 'https://www.albertamanagement.com/&perpage=all'
        yield FormRequest(start_url,
            callback=self.parseDetails)

    def parseDetails(self, response):


        apartments = response.css('.result')

        if len(apartments)>0:
            for apartment in apartments:
                external_link = 'https://www.albertamanagement.com/' + apartment.css('a::attr(href)').get()
                #rex = re.search(r'id=(\d+)',external_link)
                #external_id = str(rex.groups()[0])
        
                title = apartment.css('.title.secondary-color::text').get()
                room_count = int(apartment.css('.icon.beds::text').get()[0])
                bathroom_count = int(float(apartment.css('.icon.baths::text').get()))
                parking = True if 'Yes' in apartment.css('.icon.garage::text').get() else False
                rent = int(apartment.css('.price::text').get().replace('$',"").replace(',',""))

                
                typeText = apartment.css('.type::text').get()
                if 'Condominium' in typeText or 'Apartment' in typeText:
                    property_type = 'apartment'
                else:
                    property_type = 'house'

                dataUsage = {
                    "property_type": property_type,
                    'title':title,
                    "external_link": external_link,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "rent": rent,
                    'parking' : parking
                    }




                yield Request(external_link, meta=dataUsage,
                    callback=self.parseApartment)
        else:
            self.stopParsing = True

    def parseApartment(self,response):

        info = response.css('.features *:not(br)::text').getall()
        furnished = False
        square_meters=0
        washing_machine=False
        for idx,wrd in enumerate(info):
            if 'Furnished' in wrd:
                furnished = False if 'No' in info[idx+1] else True
            if 'Square Footage' in wrd:
                try:
                    square_meters = int(float(info[idx+1])/10.764)
                except:
                    square_meters=0
                
            if 'Laundry' in wrd:
                washing_machine = True if 'Yes' in info[idx+1] else False




        address = response.css('.info *::text').getall()[8].replace('\n',"").replace('\t',"")
        address = remove_white_spaces(address)
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        zipcode=''
        city=''
        try:
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            if not address:
                address = responseGeocodeData['address']['Match_addr']


            longitude  = str(longitude)
            latitude  = str(latitude)

        except:
            longitude=""
            latitude=""
        description = "".join(response.css('.description *::text').getall())
        description = remove_white_spaces(description)

        images = response.css('.gallery::attr(href)').getall()
        images = ['https://www.albertamanagement.com/' +x for x in images]

        available_date = response.css('.info:not(br)::text').getall()[3]

        if response.meta['rent']>0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("title", response.meta["title"])
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value('available_date',available_date)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", "CAD")
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("parking", response.meta['parking'])
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'Alberta Property Management Solutions Inc.')
            item_loader.add_value("landlord_email", 'pm@apmsi.ca')
            item_loader.add_value("landlord_phone", '1(780)715-7270')
            item_loader.add_value("position",self.position)
            self.position+=1
            yield item_loader.load_item() 