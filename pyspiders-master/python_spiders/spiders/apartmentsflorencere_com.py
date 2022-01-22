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


class Apartmentsflorencere_Spider(scrapy.Spider):

    name = 'apartmentsflorencere_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    stopParsing = False
    start_url = f'https://www.apartmentsflorencere.com/web/immobili.asp'
    def start_requests(self):

        body = {
            'num_page': '2',
            'group_cod_agenzia': '7258',
            'pagref': '0',
            'ref':'',
            'language': 'ita',
            'tipo_contratto': 'A',
            'cod_categoria': '%',
            'prezzo_min': '0',
            'prezzo_max':'16000',
            'cod_ordine': 'O02'

        }
        for i in range(1,23):
            body['num_page']=str(i)
            yield FormRequest(
                self.start_url,formdata=body,method='POST', callback=self.parse)

    def parse(self,response):

        apartments = response.css('.property-item')

        for apartment in apartments:

            property_type = 'apartment' if 'Appartamento' in  apartment.css('h4.noborder::text').get() else ''
            if property_type == '':
                continue
            rent = apartment.css('.span6 .prezzo::text').get()
            external_id = apartment.css('.rif .valore::text').get()
            square_meters = apartment.css('.area .valore::text').get()
            bathroom_count= apartment.css('.bed .valore::text').get()
            room_count    = apartment.css('.bath .valore::text').get()

            if not bathroom_count:
                bathroom_count='1'
            if not room_count:
                room_count = '1'

            url = 'https://www.apartmentsflorencere.com/' + \
                apartment.css('.span3 a::attr(href)').get()
            
            yield Request(
                url,meta={
                    'external_id':external_id,
                    'property_type':property_type,
                    'square_meters':square_meters,
                    'rent':rent,
                    'room_count':room_count,
                    'bathroom_count':bathroom_count
                },
                callback=self.parseApartment)

    def parseApartment(self, response):
        external_id = response.css('.feature-list #det_rif::attr(data-valore)').get()
        property_type = response.meta['property_type']
        square_meters =  re.search(r'\d+',response.meta['square_meters'])[0]
        try:
            rent =  re.search(r'\d+',response.meta['rent'].replace('.',''))[0]
        except:
            return
        
        title = response.css('.singleprop h1 div:nth-child(2)::text').get()
        room_count = response.meta['room_count'].replace(' ','')
        bathroom_count = response.meta['bathroom_count'].replace(' ','')
        
        
        latitude=''
        longitude=''
        city=''
        address=''
        zipcode=''
        

        try:
            map = response.css('.map-tab iframe::attr(src)').get()
            rex = re.findall(r'(-?\d+\.\d+)\,(-?\d+\.\d+)',map)
            latitude = rex[0][0]
            longitude = rex[0][1]
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
            if response.css('.feature-list #det_zona .valore::text'):
                address = response.css('.feature-list #det_zona .valore::text').get()
                if response.css('.feature-list #det_prov .valore::text'):
                    address+=',' + response.css('.feature-list #det_prov .valore::text').get()
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()

                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']

                longitude = str(longitude)
                latitude = str(latitude)
        floor = ''
        if response.css('.feature-list #det_piano::attr(data-valore)'):
            floor = response.css('.feature-list #det_piano::attr(data-valore)').get()
            if 'terra' in floor:
                floor = '1'
        furnished =True if response.css('.feature-list #det_arredato::attr(data-valore)') else False
        elevator = True if response.css('.feature-list #det_ascensore::attr(data-valore)') else False
        


        description = remove_white_spaces(
            "".join(response.css('.descrizione:not(h3)::text').getall()))

        terrace = True if 'terrazza' in description else False
        washing_machine = True if 'lavander' in description else False
        balcony = True if 'balcon' in description else False 

        images = response.css('.swiper-wrapper a::attr(href)').getall()

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

            item_loader.add_value("balcony", balcony)
            item_loader.add_value("furnished", furnished)

            item_loader.add_value("floor", floor)

            item_loader.add_value("square_meters", square_meters)
            #item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("elevator", elevator)

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
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", 'APARTMENTSFLORENCE REAL ESTATE SRL')
            item_loader.add_value("landlord_phone", '0552479309')
            item_loader.add_value("landlord_email", 'info@apartmentsflorencere.com')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
