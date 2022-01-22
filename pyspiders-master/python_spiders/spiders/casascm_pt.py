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


class casascmSpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'casascm_{locale}'
    external_source = f"casascm_PySpider_{country}_{locale}"
    allowed_domains = ['casascm.pt']

    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.casascm.pt/imoveis/apartamento/page1.html?CategoriaHidden=347&Finalidade=Aluguer',
                'property_type': 'apartment',
            },
            {
                'url': 'https://www.casascm.pt/imoveis/moradia/page1.html?CategoriaHidden=348&Finalidade=Aluguer',
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)


    def parse(self, response):

        cards = response.css(".anuncio")

        for index, card in enumerate(cards):

            if not card.css(".radius.alert.label.label-preco::text").get():
                continue

            position = self.position

            card_url = card.css("a:nth-child(1)::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)


            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            casascmSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0:
            x = re.compile(r'/page(\d+)').search(response.url)
            prevPage = int(x.groups()[0])
            nextPage = prevPage + 1
            nextPageUrl = response.url.replace(f"/page{prevPage}",f"/page{nextPage}")
            if nextPageUrl:
                yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta=response.meta)

            

    def parseApartment(self, response):
        
        rent = response.css("#PriceSpan::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent)
            rent = str(rent).split(".")[0]
        else:
            rent = None
            return

        currency = "EUR"

        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css(".radius h6 strong::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)


        square_meters = response.css(".two-up li:contains('Área Útil') strong::text, .two-up li:contains('Área Total') strong::text, .two-up li:contains('Área Bruta') strong::text").getall()
        if square_meters:
            square_meters = " ".join(square_meters)
            square_meters = remove_white_spaces(square_meters).split(".")[0]
            square_meters = extract_number_only(square_meters)

        room_count = response.css(".two-up li:contains('Quartos') strong::text, .two-up li:contains('Salas') strong::text, .two-up li:contains('Quarto') strong::text, .two-up li:contains('Sala') strong::text").getall()
        if room_count:
            room_count = " ".join(room_count)
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")

        bathroom_count = response.css(".two-up li:contains('WCs') strong::text, .two-up li:contains('WC') strong::text").getall()
        if bathroom_count:
            bathroom_count = " ".join(bathroom_count)
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count).replace(".", "")


        description = response.css("#detalhe-anuncio .descricao::text").getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None

        title = response.css("h5.titulo-anuncio::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)
        

        address = response.css(".localizacao ::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)
            
        if address and "," in address: 
            city = address.split(",")[-1].strip()
        else:
            city = address
            

        images = response.css('.fotogaleria a::attr(href)').getall()
        external_images_count = len(images)

        energy_label = response.css(".two-up li:contains('energética') strong::text").getall()
        if energy_label:
            energy_label = " ".join(energy_label)
            energy_label = remove_white_spaces(energy_label)
        
        landlord_name = response.css("#AdvertiserComercialName::text").getall()
        if landlord_name:
            landlord_name = " ".join(landlord_name)
            landlord_name = remove_white_spaces(landlord_name)

        landlord_phone = "+351228346500"
        
        landlord_email = response.css(".show-for-medium-up::text").getall()
        if landlord_email:
            landlord_email = " ".join(landlord_email)
            landlord_email = remove_white_spaces(landlord_email)

        if rent:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
