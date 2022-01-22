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


class hamiltonmaySpider(scrapy.Spider):

    name = 'hamiltonmay_pl'
    execution_type = 'testing'
    country = 'poland'
    locale = 'pl'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['hamiltonmay.com']

    position = 1
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.hamiltonmay.com/properties/search?branchId=all&development=&type=apartment&want=rent&page=1',
                'property_type': 'apartment',
            },
            {
                'url': 'https://www.hamiltonmay.com/properties/search?branchId=all&development=&type=house&want=rent&page=1',
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):

        cards = response.css(".tabcontent .media")

        for index, card in enumerate(cards):

            position = self.position

            card_url = card.css("a.listing-btn:contains('more details')::attr(href)").get()

            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            hamiltonmaySpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        if len(cards) > 0:
                
            prev_page = int(parse_qs(response.url)['page'][0])
            next_page = int(parse_qs(response.url)['page'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&page={prev_page}",f"&page={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True, meta=response.meta)

    def parseApartment(self, response):
        rent = response.css(".property-total-value::text, .col-12.widget-content:contains('rental price from') .float-right::text").get()
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
        else:
            rent = None
            return

        currency = "PLN"

        property_type = response.meta['property_type']

        position = response.meta['position']


        external_id = response.css("[title='PPi']::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id).replace(" ", "")
            external_id = extract_number_only(external_id).replace(".", "")



        square_meters = response.css(".col-12.widget-content:contains('Size') ::text").getall()
        if square_meters:
            square_meters = " ".join(square_meters)
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters).replace(".", "")

        room_count = response.css(".col-12.widget-content:contains('Bedrooms') ::text").getall()
        if room_count:
            room_count = " ".join(room_count)
            if "studio" in room_count:
                room_count = 1
            else:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count).replace(".", "")

        bathroom_count = response.css(".col-12.widget-content:contains('bathrooms') ::text").getall()
        if bathroom_count:
            bathroom_count = " ".join(bathroom_count)
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count).replace(".", "")

        description = response.css("div.white-container  div:nth-child(2) > div.col-md-8.col-lg-9 ::text").getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None



        city = response.css(".col-12.widget-content:contains('City') .float-right ::text").getall()
        if city:
            city = " ".join(city)
            city = remove_white_spaces(city)

        district = response.css(".col-12.widget-content:contains('Region') .float-right ::text").getall()
        if district:
            district = " ".join(district)
            district = remove_white_spaces(district)
            
        street = response.css(".col-12.widget-content:contains('Street') .float-right ::text").getall()
        if street:
            street = " ".join(street)
            street = remove_white_spaces(street)

        address = f"{street}, {district}, {city}, Poland"
        
        title = response.css(".single-property h2::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)
        
        
        
        latitude = response.css(".maps::attr(data-lat)").get()
        longitude = response.css(".maps::attr(data-lng)").get()
        
        zipcode = None

        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            zipcode = None


        images = response.css('#links a::attr(href)').getall()
        external_images_count = len(images)

        floor = response.css(".col-12.widget-content:contains('level') .float-right ::text").getall()
        if floor:
            floor = " ".join(floor)
            floor = remove_white_spaces(floor)

        amenities = response.css(".amenities .amenity-tooltip::attr(title)").getall()
        if amenities:
            amenities = " ".join(amenities).lower()

         
        elevator = "lift" in amenities
        if elevator:
            elevator = True
        else:
            elevator = False

        balcony = "balcony" in amenities
        if balcony:
            balcony = True
        else:
            balcony = False

        
        parking = "parking" in amenities
        if parking:
            parking = True
        else:
            parking = False
            
        swimming_pool = "swimming pool" in amenities
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False
        
        
        pets_allowed = "pet friendly" in amenities
        if pets_allowed:
            pets_allowed = True
        else:
            pets_allowed = False
        
        
        mail = ["krakow@hamiltonmay.com", "warsaw@hamiltonmay.com"]
        landlord_name = " Hamilton May"
        
        landlord_email = "warsaw@hamiltonmay.com"
        
        landlord_phone = response.css("body > div.single-property-content > div.white-container > div > div:nth-child(2) > div.col-md-4.col-lg-3.order-first.order-md-last > div:nth-child(1) > div > div.col-md-10.col-lg-10 > div > div:nth-child(2)::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")

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
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
