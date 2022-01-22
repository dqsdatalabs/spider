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


class scianyNieruchomosciSpider(scrapy.Spider):

    name = 'scianyNieruchomosci_pl'
    execution_type = 'testing'
    country = 'poland'
    locale = 'pl'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['4sciany-nieruchomosci.pl']

    position = 1
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'http://4sciany-nieruchomosci.pl/lista-ofert/?f_location_locality%5B%5D=&mapa=&mapaX1=&mapaY1=&mapaX2=&mapaY2=&mapaCX=&mapaCY=&mapaCZ=0&mapaVis=0&f_street_name=&f_sectionName1=Apartment&f_sectionName2=Rental&f_totalAreaMin=&f_totalAreaMax=&f_noOfRoomsMin=&f_noOfRoomsMax=&f_listingId=&f_price_amountMin=0&f_price_amountMax=&submit=Search',
                
                'property_type': 'apartment',
            },
            {
                'url': 'http://4sciany-nieruchomosci.pl/lista-ofert/?f_location_locality%5B%5D=&mapa=&mapaX1=&mapaY1=&mapaX2=&mapaY2=&mapaCX=&mapaCY=&mapaCZ=0&mapaVis=0&f_street_name=&f_sectionName1=House&f_sectionName2=Rental&f_totalAreaMin=&f_totalAreaMax=&f_noOfRoomsMin=&f_noOfRoomsMax=&f_price_amountMin=0&f_price_amountMax=&submit=Search',
                
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), headers=self.headers, callback=self.parse, dont_filter=True, meta=url)


    def parse(self, response):

        cards = response.css("#properties .property")

        for index, card in enumerate(cards):
            position = self.position

            card_url = card.css("aside a.link-arrow::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)

            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            scianyNieruchomosciSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, headers=self.headers, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css(".pagination li.active + li a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url=nextPageUrl, headers=self.headers, callback=self.parse, dont_filter=True, meta=response.meta)

    def parseApartment(self, response):

        rent = response.css("#quick-summary dt:contains('Cena') + dd span::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
        else:
            return

        currency = "PLN"

        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css("#quick-summary dt:contains('Numer oferty') + dd::text").get()
        if external_id:
            external_id = remove_tags(external_id)
            external_id = remove_white_spaces(external_id)

        square_meters = response.css("#quick-summary dt:contains('Powierzchnia') + dd::text").get()
        if square_meters:
            square_meters = remove_tags(square_meters)
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters).split(".")[0]


        room_count = response.css("#quick-summary dt:contains('Liczba pokoi') + dd::text").get()
        if room_count:
            room_count = remove_tags(room_count)
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")


        description = response.css("#description::text, #description div::text").getall()
        description = " ".join(description)
        if description:
            description = remove_white_spaces(description)
        else:
            description = None
            
        city = response.css(".property-title h1::text").get()
        if city:
            city = remove_tags(city)
            city = remove_white_spaces(city)
        
        street = response.css(".property-title h2::text").get()
        if street:
            street = remove_tags(street)
            street = remove_white_spaces(street)
        
        if street:    
            address = f"{street} - {city} - poland"
        else: 
            address = f"{city} - poland"
    
        title = f"{address} - {external_id}"

        images = response.css('.property-gallery-bigImageFeed a.colorbox::attr(href)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)

        floor = response.css("#quick-summary dt:contains('Piętro') + dd::text").get()
        if floor:
            floor = remove_tags(floor)
            floor = remove_white_spaces(floor)
            floor = extract_number_only(floor).replace(".", "")

        elevator = response.css("#quick-summary dt:contains('Winda') + dd::text").get()
        if elevator:
            elevator = True
        else:
            elevator = False

        balcony = response.css("#quick-summary dt:contains('Liczba balkonów') + dd::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False

        parking = response.css("#quick-summary dt:contains('Garaż') + dd::text").get()
        if parking:
            parking = True
        else:
            parking = False
        
        landlord_name = response.css(".agent-info h3::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
        
        landlord_phone = response.css(".agent-info dl dt:contains('Telefon:') + dd a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")
            
        landlord_email = response.css(".agent-info dl dt:contains('E-mail:') + dd a::text").get()
        if landlord_email:
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
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
