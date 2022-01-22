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

class ImmobiliarezecchinoSpider(scrapy.Spider):

    name = 'immobiliarezecchino'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['immobiliarezecchino.it']
    start_urls = ['https://www.immobiliarezecchino.it/it/list.php?idCat=1022&type=1&page=1']

    position = 1
    def parse(self, response):
        pages = response.css('.pagination .page-numbers::attr(href)').getall()
        pages.insert(0,"?idCat=1022&type=1&page=1")
        for page in pages:
            if len(page)>30:
                break
            yield Request(
            url ="https://www.immobiliarezecchino.it/it/list.php"+page,
             callback=self.parseApartment)


    def parseApartment(self, response):

        
        apartments = response.css('.btn-primary::attr(href)').getall()
        apartments.pop(0)
        for apartment in apartments:
            yield Request("https://www.immobiliarezecchino.it/it/"+apartment,callback=self.parseDetails)
        





    def parseDetails(self,response):

        print('='*50)
        external_id = response.url[response.url.find("id=")+3:len(response.url)]
        title = response.css('.entry-title.pull-left::text').get()
        
        descriptionText = response.css('.im-readAll__container.js-readAllContainer *::text').getall()
        if not descriptionText:
            descriptionText = response.css('..entry-content *::text').getall()
        description = ''
        for text in descriptionText:
            description+=text.replace('\n,\t',"")
        remove_white_spaces(description)

        listInfo = response.css('.list-info li *::text').getall()
        for i in range(0,len(listInfo)):
            
            if i<len(listInfo) and listInfo[i]==' ':
                del listInfo[i]
                i-=1
            
        del listInfo[2]
        square_meters=0
        energy_label = ''
        floor = ''
        elevator=False

        for i in range(0,len(listInfo)):
            if 'Superficie' in listInfo[i]:
                digits = [int(s) for s in listInfo[i+1].split() if s.isdigit()]
                if len(digits)>0:
                    square_meters = int(digits[0])
            
            if 'energ' in listInfo[i]:
                energy_label = listInfo[i+1].replace(' ',"")
            if 'Piano:' in listInfo[i]:
                floor = listInfo[i+1][1:]
            if 'Ascensore' in listInfo[i]:
                elevator = True if 'SI' in listInfo[i+1] else False

        
        address = response.css('.property-view-map::text').get()

     
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        longitude=''
        latitude=''
        zipcode=''
        city=''
        if len(responseGeocodeData['locations'])>0:
            longitude = str(responseGeocodeData['locations'][0]['feature']['geometry']['x'])
            latitude = str(responseGeocodeData['locations'][0]['feature']['geometry']['y'])

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']


        rent = response.css('.property-price span::text').get()
        remove_white_spaces(rent)
        price = re.findall('\d+.\d+',rent)
        price = price[0].replace(',00',"").replace('.',"")
        rent = int(price)
        currency = "EUR"

        bathroom_count = int(response.css('.property-label-bathrooms *::text').getall()[0])
        try:
            room_count = int(response.css('.property-label-bedrooms *::text').getall()[0])
        except:
            room_count=1
        
       

        images = response.xpath('//*[@id="sync1"]//@href').getall()
        property_type='apartment'

        #print(listInfo)
        
    
        print(response.url)
        print('='*50)

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
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("energy_label", energy_label)
        #item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        #item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        #item_loader.add_value("balcony", balcony)
        '''
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)'''
        item_loader.add_value("position", self.position)

        self.position+=1

        yield item_loader.load_item()