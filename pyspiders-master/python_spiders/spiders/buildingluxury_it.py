from operator import le
from types import MethodType
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


class Buildingluxury_Spider(scrapy.Spider):

    name = 'buildingluxury'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    stopParsing = False
    start_url = f'https://www.buildingluxury.it/immobili/RESIDENZIALE-AFFITTI-FI.html'
    def start_requests(self):

        body = {
            'num_page': '5',
            'group_cod_agenzia': '5201',
            'pagref': '71376',
            'ref':'RESIDENZIALE-AFFITTI-FI',
            'language': 'ita',
            'tipo_contratto': 'A',
            'cod_categoria': 'R',
            'cod_provincia': '32',
            'cod_comune': '0'
        }
        for i in range(1,18):
            body['num_page']=str(i)
            yield FormRequest(
                self.start_url,formdata=body,method='POST', callback=self.parse)

    def parse(self,response):
  
        apartments = response.css('.annuncio')

        for apartment in apartments:
            url = 'https://www.buildingluxury.it' + \
                apartment.css('a::attr(href)').get()
            
            title         = apartment.css('h4::text').get()
            square_meters = apartment.css('.features .area::text').get()
            rent          = apartment.css('.features .prezzodiff::text').get()
            bathroom_count= apartment.css('.features .bath::text').get()
            room_count    = apartment.css('.features .bed::text').get()
            if not bathroom_count:
                bathroom_count='1'
            if not room_count:
                room_count = '1'
            
            yield Request(
                url,meta={
                    'title':title,
                    'square_meters':square_meters,
                    'rent':rent,
                    'room_count':room_count,
                    'bathroom_count':bathroom_count
                },
                callback=self.parseApartment, dont_filter=True)

    def parseApartment(self, response):
        title = response.meta['title']
        square_meters =  re.search(r'\d+',response.meta['square_meters'])[0]
        try:
            rent =  re.search(r'\d+',response.meta['rent'].replace('.',''))[0]
        except:
            return
        
        title = response.meta['title']
        room_count = response.meta['room_count'].replace(' ','')
        bathroom_count = response.meta['bathroom_count'].replace(' ','')

        energy_label = response.css('#li_clen::text').get().replace(': ','')
        if response.css("#det_arredato .valore"):
            furnished = True 
        else:
            furnished = False

        if response.css("#det_ascensore .valore"):
            elevator = True 
        else:
            elevator = False
        if response.css("#det_parcheggio .valore"):
            parking = True 
        else:
            parking = False
        external_id = response.css('.sfondo_colore3.colore1 strong::text').get()
        if response.css('#det_piano .valore::text'):
            floor = response.css('#det_piano .valore::text').get()
            if 'terra' in floor:
                floor = 'Ground floor'
            else:
                try:
                    floor = re.search(r'\d+',floor)[0]
                except:
                    pass
        
        map = response.css('.map-tab a::attr(href)').get()
        rex = re.findall(r'(-?\d+\.\d+)\,(-?\d+\.\d+)',map)
        latitude = rex[0][0]
        longitude = rex[0][1]

        try:
            '''responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']'''

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)
        except:
            pass

        description = remove_white_spaces(
            "".join(response.css('.imm-det-des *::text').getall()))
        if 'appartamento' in description:
            property_type='apartment'
        else:
            property_type='house'

        '''dishwasher = True if response.css(
            ".table tbody tr *:contains(Dishwasher) strong") else False
        washing_machine = True if response.css(
            ".table tbody tr *:contains(Washer) strong") else False
        parking = True if response.css(
            ".table tbody tr *:contains(Parking) strong") else False
        #pets_allowed = False if 'No Pets' in description else True
        balcony = True if 'balcone' in description else False #done
        furnished = True if 'Furnished' in description else False
        landlord_phone = response.css(
            ".table tbody tr *:contains(Phone) strong::text").get()
        landlord_email = response.css(
            ".table tbody tr *:contains(Email) strong a::text").get()
        landlord_name = 'metro property management'
        available_date = response.css(
            ".table tbody tr *:contains(Date) strong::text").get()
        if response.css(".office:contains('Office') span"):
            landlord_phone = response.css(
                ".office:contains('Office') span::text").get().replace('Office: ', '')
        else:
            landlord_phone = '416-565-3001'''

        images = response.css('.slides li a::attr(data-img)').getall()
        if 'garage' in response.url:
            return

        if int(rent) > 0:

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

            #item_loader.add_value("balcony", balcony)
            item_loader.add_value("furnished", furnished)

            item_loader.add_value("elevator", elevator)

            item_loader.add_value("square_meters", square_meters)
            #item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("energy_label", energy_label)

            #item_loader.add_value('available_date', available_date)
            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value("parking", parking)
            #item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'BUILDINGLUXURY - REAL ESTATE')
            item_loader.add_value("landlord_phone", '055/0736856')
            #item_loader.add_value("landlord_email", landlord_email)

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
