# -*- coding: utf-8 -*-
# Author: Ahmed Atef
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
from w3lib.html import remove_tags


class casaexpressSpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'casaexpress_{locale}'
    external_source = f"casaexpress_PySpider_{country}_{locale}"
    allowed_domains = ['casaexpress.pt']

    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }
    

    def start_requests(self):
        start_urls = [
            {
                'url': 'http://www.casaexpress.pt/imoveis/imoveis.htm?p=imoveis&f=imoveis&lang=pt&PQ=1&pag=1&NA=11%2C1&DD=0&PN=&PX=&TN=&AN=&AX=&NG=2%2C8&ref=&E=0',
                'property_type': 'apartment',
            },
            {
                'url': 'http://www.casaexpress.pt/imoveis/imoveis.htm?p=imoveis&f=imoveis&lang=pt&PQ=1&pag=1&NA=32%2C17%2C18%2C16%2C14%2C11&DD=0&TN=&NG=2%2C8&E=0&AN=&AX=&PN=&PX=&Ref=',
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), headers=self.headers, callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):
    
        
        cards = response.css(".t12.verde26 + div > a")
        
        
        for index, card in enumerate(cards):
            

            position = self.position

            card_url = card.css("::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)


            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }


            casaexpressSpider.position += 1
            yield Request(card_url, headers=self.headers, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0:
            nextPageUrl = response.css(".paginacao_on:contains('seguinte')::attr(href)").get()
            if nextPageUrl:
                nextPageUrl = response.urljoin(nextPageUrl)

            if nextPageUrl and nextPageUrl != response.url:
                yield Request(url = nextPageUrl, headers=self.headers, callback = self.parse, dont_filter = True, meta=response.meta)

    def parseApartment(self, response):
        
        rent = response.css(".precos.t26.myriadR.verde26.bold::text").get()
        if rent:
            rent = remove_white_spaces(rent)
            rent = extract_number_only(rent)
            if not rent:
                return
        else:
            return

        currency = "EUR"

      
        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css(".barra_icons td:contains('Adicionar Imóvel') div::attr(id)").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id)


        square_meters = response.css(".caracteristica-item:nth-child(2) .item_texto::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters).split(".")[0]
            square_meters = extract_number_only(square_meters)


        room_count_text = response.css(".myriadR.verde26.t32.titulo::text").get()
        if room_count_text:
            room_count = remove_white_spaces(room_count_text)
            room_count = extract_number_only(room_count)
            if room_count == '0':
                room_count = room_count_text[::-1]
                room_count = extract_number_only(room_count)


        description = response.css(".descricao.t12.cinza33.lh150::text").getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None

        title = response.css(".myriadR.verde26.t32.titulo::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)
        

        address = response.css(".muriadR.t14.verde4D::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)
            
        city = address.split("-")[0]
        if city:
            city = remove_white_spaces(city)
            
            
        zipcode = None
        longitude = None
        latitude = None
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r'loadMInter\((\-?\d*\.?\d*),(\-?\d*\.?\d*),')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]
        
        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                address = responseGeocodeData['address']['LongLabel']
                city = responseGeocodeData['address']['City']
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            pass
        
        images = response.css('.thumb_holder tr img::attr(src)').getall()
        images = [re.sub(r'_ico.jpg', "", img) for img in images]
        external_images_count = len(images)

        energy_label_table={
            "1": "unkonwn",
            "2": "F",
            "3": "E",
            "4": "D",
            "5": "C",
            "6": "B-",
            "7": "B",
            "8": "A",
            "9": "A+",
            "10": "F",
        }
        energy_label = response.css("#energia img::attr(src)").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label).split("/")[-1]
            energy_label = extract_number_only(energy_label)
            energy_label = energy_label_table[energy_label]
            
            
        furnished = response.css(".sans_verde_claro_11 div:contains('Moveis')").get()
        if furnished:
            furnished = True
        else:
            furnished = False
        

        balcony = response.css(".caract_item:contains('Varanda')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False
        
        washing_machine = response.css(".caract_item:contains('Máquina lavar louça')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False

        
        landlord_name = response.css("#mediadora_max .verde26 ::text").getall()
        if landlord_name:
            landlord_name = " ".join(landlord_name)
            landlord_name = remove_white_spaces(landlord_name)


        if rent:
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
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("position", position)

            yield item_loader.load_item()




