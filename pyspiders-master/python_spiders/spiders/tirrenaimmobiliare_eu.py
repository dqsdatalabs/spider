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


class Tirrenaimmobiliare_Spider(scrapy.Spider):

    name = 'tirrenaimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        url = 'https://www.tirrenaimmobiliare.eu/r-immobili/?cf=yes&Motivazione%5B%5D=&Motivazione%5B%5D=2&pisa=&fuoripisa='
        yield Request(url, callback=self.parseDetails)

    def parseDetails(self, response):
        apartments = response.css('.list-halfmap-mappa')

        for apartment in apartments:
            external_id = apartment.css('.codice::text').get().replace('Rif. ','')
            title = "".join(apartment.css(".titolo:not(br)::text").getall())
            if 'commerciale' in title:
                continue

            if apartment.css(".icone span:contains('mq')::text"):
                square_meters = apartment.css(".icone span:contains('mq')::text").get()
            else:
                square_meters = 0

            if apartment.css(".icone span:contains('Camere')::text"):
                room_count = apartment.css(".icone span:contains('Camere')::text").get()[0]
            else:
                room_count = '1'

            if apartment.css(".icone span:contains('Bagn')::text"):
                bathroom_count = apartment.css(".icone span:contains('Bagn')::text").get()[0]
            else:
                bathroom_count = '1'

            external_link = apartment.css("figure a::attr(href)").get()
            rent = 0
            try:
                rent = int(re.search(
                    r'\d+', apartment.css('.prezzo::text').get().replace('.', ''))[0])
            except:
                return

            datausge = {
                'external_id':external_id,
                'title': remove_white_spaces(title),
                'rent': rent,
                'room_count': room_count,
                'bathroom_count':bathroom_count,
                'square_meters':square_meters
            }
            yield Request(external_link, callback=self.parseApartment, meta=datausge)

    def parseApartment(self, response):

        title = response.meta['title']
        rent = response.meta['rent']
        external_id = response.meta['external_id']
        room_count = response.meta['room_count']
        bathroom_count = response.meta['bathroom_count']
        square_meters = response.meta['square_meters']


        description = remove_white_spaces(
            "".join(response.css('.testo *::text').getall()))
      
        energy_label = response.css('.liv_classe::text').get()

        if  response.css(".schedaMobile .box:contains('Ascensore: Si')::text"):
            elevator = True
        else:
            elevator = False
        
        if  response.css(".schedaMobile .box:contains('Terrazzo')::text"):
            terrace = True
        else:
            terrace = False

        if  response.css(".schedaMobile .box:contains('Arredato')::text"):
            furnished = True
        else:
            furnished = False

        if response.css(".schedaMobile .box:contains('Piano')"):
            floor = response.css(".schedaMobile .box:contains('Piano')::text").get()
        else:
            floor = ''
        
        utilities = re.search(r'\d+',response.css(".schedaMobile .box:contains('Spese condominio')::text").get())[0]

        
        rex = re.search(r'\d+',floor)
        if rex:
            floor=rex[0]

        if response.css(".schedaMobile .box:contains('Posto auto:')"):
            parking = True
        else:
            parking = False

        latitude = response.css("meta[itemprop='latitude']::attr(content)").get()
        longitude = response.css("meta[itemprop='longitude']::attr(content)").get()

        zipcode = ''
        city = ''
        address = ''
        try:
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['LongLabel']
        except:
            pass

        imageId = response.xpath("//*[@id='formSchedaImmo']/input[6]").css("::attr(value)").get()

        datausge = {
                'external_id':external_id,
                'title': remove_white_spaces(title),
                'rent': rent,
                'room_count': room_count,
                'bathroom_count':bathroom_count,
                'square_meters':square_meters,
                'description':description,
                'external_link':response.url,
                'zipcode':zipcode,
                'city':city,
                'address':address,
                'floor':floor,
                'longitude':longitude,
                'latitude':latitude,
                'furnished':furnished,
                'elevator':elevator,
                'utilities':utilities,
                'terrace':terrace,
                'energy_label':energy_label,
                'parking':parking
        }

        yield FormRequest('https://www.tirrenaimmobiliare.eu/moduli/swiper_foto.php',meta=datausge,
        formdata={
            'id_slider':imageId,
            'sezione':'scheda-immobili',
            'pagination':'number'
        },
        method='POST',callback=self.parseImages)

    def parseImages(self,response):

        images =  response.css('.swiper-slide img::attr(src)').getall()
        images = [x.replace('/thumb','') for x in images]

        external_id                =response.meta['external_id']
        title  =response.meta['title']
        rent          =response.meta['rent']
        room_count    =response.meta['room_count']
        bathroom_count=response.meta['bathroom_count']
        square_meters =response.meta['square_meters']
        description   =response.meta['description']
        external_link  =response.meta['external_link']
        zipcode       =response.meta['zipcode']
        city          =response.meta['city']
        address       =response.meta['address']
        floor         =response.meta['floor']
        latitude         =response.meta['latitude']
        longitude         =response.meta['longitude']
        furnished     =response.meta['furnished']
        elevator      =response.meta['elevator']
        terrace       =response.meta['terrace']
        energy_label  =response.meta['energy_label']
        parking  =response.meta['parking']
        utilities  =response.meta['utilities']

        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", external_link)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value('energy_label', energy_label)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value('terrace', terrace)

            item_loader.add_value('furnished', furnished)
            item_loader.add_value('floor', floor)
            item_loader.add_value('address', address)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('parking', parking)
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

            item_loader.add_value(
                "property_type", 'apartment')
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("balcony", 'balcon' in description)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value('elevator', elevator)

            item_loader.add_value("landlord_name", 'TIRRENA Immobiliare Snc')
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("landlord_phone", '050-775700')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
