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


class PrestigeRealEstateSpider(scrapy.Spider):

    name = 'PrestigeRealEstate_pl'
    execution_type = 'testing'
    country = 'poland'
    locale = 'pl'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['prestige-real-estate.pl']

    position = 1

    def start_requests(self):
        start_urls = [
            {'url': 'https://prestige-real-estate.pl/offers/?tn=1&tt=2&p=1',
             'property_type': 'house',
            },
            {'url': 'https://prestige-real-estate.pl/offers/?tn=2&tt=2&p=1',
             'property_type': 'apartment',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)


    def parse(self, response):

        cards = response.css("#offerlist .offer-list .offer-list2 > .row")

        for index, card in enumerate(cards):

            position = self.position

            card_url = card.css(".offer-label a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)

            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            PrestigeRealEstateSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css(".pglower .pagination li.current + li a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True, meta=response.meta)


    def parseApartment(self, response):

        rent = response.css(".the-price::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
        else:
            return

        currency = "PLN"

        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css(".subheader.twentypx").get()
        if external_id:
            external_id = remove_tags(external_id)
            external_id = remove_white_spaces(external_id)
            external_id = extract_number_only(external_id).replace(".", "")

        square_meters = response.css(".priceMain span.offer-font1").get()
        if square_meters:
            square_meters = remove_tags(square_meters)
            square_meters = remove_white_spaces(square_meters)

        room_count = response.css(".group-data li:contains('Rooms')").get()
        if room_count:
            room_count = remove_tags(room_count)
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")

        description = response.css(".offer-desc p::text").getall()
        description = " ".join(description)
        if description:
            description = remove_white_spaces(description)
        else:
            description = None

        bathroom_count = description
        if bathroom_count:
            pattern = re.compile(r'(\d+)?\+?\s?[a-zA-Z\s]*? (bathrooms|bath|bathroom|łazienki)', re.IGNORECASE)
            data_from_regex = pattern.search(bathroom_count)
            if data_from_regex:
                bathroom_count = data_from_regex.group(1)
            else:
                bathroom_count = None


        title = response.css(".hg h1::text").get()
        if title:
            title = remove_white_spaces(title)

        city = title.split(" ")[1]
        
        district = response.css(".group-data li:contains('District')::text").getall()
        district = " ".join(district)
        if district:
            district = remove_white_spaces(district)
        
        street = response.css(".group-data li:contains('Street')::text").getall()
        street = " ".join(street)
        if street:
            street = remove_white_spaces(street)
        
        if street:    
            address = f"{street} - {district} - {city} - poland"
        else: 
            address = f"{district} - {city} - poland"

        images = response.css('#slider .slides img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)
        
        if property_type == "apartment":
            floor = response.css(".group-data li:contains('Floor')").getall()
            if len(floor)>0:
                floor = floor[-1]
                floor = remove_tags(floor)
                floor = remove_white_spaces(floor)
                floor = extract_number_only(floor)
        else:
            floor = None

        balcony = "balcony" in description or "balkon" in description
        if balcony:
            balcony = True
        else:
            balcony = False

        terrace = "terrace" in description or "taras" in description or "tarasem" in description
        if terrace:
            terrace = True
        else:
            terrace = False
        
        parking = "garage" in description or "garaż" in description or "garażowe" in description
        if parking:
            parking = True
        else:
            parking = False
            
        furnished = "furnished" in description or "umeblowane" in description
        if furnished:
            furnished = True
        else:
            furnished = False

        washing_machine = "laundry" in description or "pranie" in description or "pralnia" in description
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False
        
        landlord_phone = response.css(".callout p a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")

        landlord_name = response.css(".callout p::text").get()
        if landlord_name:
            landlord_name = landlord_name.split("Phone")[0]
            landlord_name = remove_white_spaces(landlord_name)
        
        if landlord_name:
            landlord_email = landlord_name.split(" ")[0].lower()
            if landlord_email:
                landlord_email = f"{landlord_email}@nprestige.pl"
                
        if not landlord_email:
            landlord_email = "biuro@nprestige.pl"
        if not landlord_phone:
            landlord_phone = "+48733592900"

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
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
