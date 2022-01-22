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


class greenAcresSpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'greenAcres_{locale}'
    external_source = f"greenAcres_PySpider_{country}_{locale}"
    allowed_domains = ['green-acres.pt']

    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }
    
    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_lisboa',
                'property_type': 'apartment',
                'region': 'Lisbon',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_lisboa',
                'property_type': 'house',
                'region': 'Lisbon',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_madeira',
                'property_type': 'apartment',
                'region': 'Funchal',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_madeira',
                'property_type': 'house',
                'region': 'Funchal',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_faro',
                'property_type': 'apartment',
                'region': 'Faro',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_faro',
                'property_type': 'house',
                'region': 'Faro',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_beja',
                'property_type': 'apartment',
                'region': 'Beja',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_beja',
                'property_type': 'house',
                'region': 'Beja',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_setubal',
                'property_type': 'apartment',
                'region': 'Setúbal',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_setubal',
                'property_type': 'house',
                'region': 'Setúbal',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_evora',
                'property_type': 'apartment',
                'region': 'Evora',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_evora',
                'property_type': 'house',
                'region': 'Evora',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_portalegre',
                'property_type': 'apartment',
                'region': 'Portalegre',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_portalegre',
                'property_type': 'house',
                'region': 'Portalegre',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_santarem',
                'property_type': 'apartment',
                'region': 'Santarém',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_santarem',
                'property_type': 'house',
                'region': 'Santarém',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_castelo_branco',
                'property_type': 'apartment',
                'region': 'Castelo Branco',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_castelo_branco',
                'property_type': 'house',
                'region': 'Castelo Branco',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_leiria',
                'property_type': 'apartment',
                'region': 'Leiria',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_leiria',
                'property_type': 'house',
                'region': 'Leiria',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_coimbra',
                'property_type': 'apartment',
                'region': 'Coimbra',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_coimbra',
                'property_type': 'house',
                'region': 'Coimbra',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_guarda',
                'property_type': 'apartment',
                'region': 'Guarda',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_guarda',
                'property_type': 'house',
                'region': 'Guarda',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_braganca',
                'property_type': 'apartment',
                'region': 'Braganca',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_braganca',
                'property_type': 'house',
                'region': 'Braganca',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_viseu',
                'property_type': 'apartment',
                'region': 'viseu',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_viseu',
                'property_type': 'house',
                'region': 'viseu',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_aveiro',
                'property_type': 'apartment',
                'region': 'aveiro',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_aveiro',
                'property_type': 'house',
                'region': 'aveiro',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_porto',
                'property_type': 'apartment',
                'region': 'porto',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_porto',
                'property_type': 'house',
                'region': 'porto',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_vila_real',
                'property_type': 'apartment',
                'region': 'vila real',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_vila_real',
                'property_type': 'house',
                'region': 'vila real',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_braga',
                'property_type': 'apartment',
                'region': 'braga',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_braga',
                'property_type': 'house',
                'region': 'braga',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_appartement-on-city_id-rg_viana_do_castelo',
                'property_type': 'apartment',
                'region': 'viana do castelo',
            },
            {
                'url': 'https://www.green-acres.pt/pt/prog_show_rentals.html?searchQuery=lg-pt-cn-pt-type-rentals-hab_castle-on-hab_house-on-city_id-rg_viana_do_castelo',
                'property_type': 'house',
                'region': 'viana do castelo',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):
    
        
        check = response.css("h2.title-standard::text").get()
        if check:
            return
        
        cards = response.css("#adverts-grid-container .item-main")

        for index, card in enumerate(cards):

            position = self.position

            card_url = card.css("a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)


            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            greenAcresSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css("#nextPage::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl and nextPageUrl != response.url:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta=response.meta)
            

    def parseApartment(self, response):
        rent = response.css("p.form-price::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
            rent = str(rent).split(".")[0]
        else:
            rent = None
            return

        currency = "EUR"

        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css("#contactAdvertId::attr('value')").get()
        if external_id:
            external_id = remove_white_spaces(external_id)



        square_meters = response.css(".item-content-part.main-characteristics ul li p:contains('superfície habitável')::text, .item-content-part.main-characteristics ul li p:contains('terreno')::text").getall()
        if square_meters:
            square_meters = " ".join(square_meters)
            square_meters = remove_white_spaces(square_meters).split(",")[0]
            square_meters = extract_number_only(square_meters).replace(".", "")

        room_count = response.css(".item-content-part.main-characteristics ul li p:contains('quartos')::text, .item-content-part.main-characteristics ul li p:contains('quarto')::text").getall()
        if room_count:
            room_count = " ".join(room_count)
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")

        bathroom_count = response.css(".item-content-part.main-characteristics ul li p:contains('casa de banho')::text, .item-content-part.main-characteristics ul li p:contains('casas de banho')::text").getall()
        if bathroom_count:
            bathroom_count = " ".join(bathroom_count)
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count).replace(".", "")


        description = response.css("#DescriptionDiv p.text::text").getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None




        title = response.css("h1.item-title::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)
        

        address = response.css("#mainInfoAdvertPage a.item-location p.details-name::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)
            
        city = None
        zipcode = None
        longitude = None
        latitude = None
        
        
        script_map = response.css(".advert-page.property-content.slide script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            script_map = remove_white_spaces(script_map)
            pattern = re.compile(r'coordinates: {"latitude":(\-?\d*\.?\d*),"longitude":(\-?\d*\.?\d*)}')
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
            city = None
            zipcode = None




        if script_map:
            script_map = remove_white_spaces(script_map)
            # pattern = re.compile(r'var bigPhotos = \[(.*)\]\;')
            pattern = re.compile(r'var bigPhotos = \[(.*)\]; var smallPhotos =')
            x = pattern.search(script_map)
            imagesData = x.groups()[0]
            images = imagesData.split(",")
            images = [re.sub(r'"', '', img) for img in images]
            external_images_count = len(images)

        parking = response.css(".item-content-part.main-characteristics ul li p:contains('estacionamento')").get()
        if parking:
            parking = True
        else:
            parking = False
            
        furnished = response.css(".item-content-part.main-characteristics ul li p:contains('Mobilado')").get()
        if furnished:
            furnished = True
        else:
            furnished = False
        
        elevator = response.css(".item-content-part.main-characteristics ul li p:contains('Elevador')").get()
        if elevator:
            elevator = True
        else:
            elevator = False

        terrace = response.css(".item-content-part.main-characteristics ul li p:contains('Terraço')").get()
        if terrace:
            terrace = True
        else:
            terrace = False
        
        swimming_pool = response.css(".item-content-part.main-characteristics ul li p:contains('Piscina')").get()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False
        
        landlord_name = response.css(".item-content-part.price.item-ecology p:contains('Agência')::text").getall()
        if landlord_name:
            landlord_name = " ".join(landlord_name)
            landlord_name = remove_white_spaces(landlord_name).replace("Agência: ","")

        landlord_phone = "+351308807184"

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
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
