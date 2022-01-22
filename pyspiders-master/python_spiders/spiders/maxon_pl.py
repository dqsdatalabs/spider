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


class maxonSpider(scrapy.Spider):

    name = 'maxon_pl'
    execution_type = 'testing'
    country = 'poland'
    locale = 'pl'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['maxon.pl']

    position = 1
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.maxon.pl/mieszkania/oferty/mieszkania/wynajem?&Page=1',
                'property_type': 'apartment',
            },
            {
                'url': 'https://www.maxon.pl/mieszkania/oferty/dom/wynajem?&Page=1',
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), headers=self.headers, callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):
        script_data = response.css("script::text").getall()
        script_data = "\n ".join(script_data)
        if script_data:
            pattern = re.compile(r'mapViewModel = new ListViewModel\(([\w\W]+)?\);\s+ko.applyBindings\(mapViewModel\);')
            x = pattern.search(script_data)
            script_data = x.groups()[0]
            jsonData = json.loads(script_data)
        
        cards = jsonData['PreloadedData']['Items']

        for index, card in enumerate(cards):


            position = self.position

            card_url = card["URL"]
            
            ID = card["IDS"]
            Title = card["Title"]
            rent = card["IMD"]
            square_meters = card["A"]
            
            Street = card["Street"]
            Section = card["Section"]
            City = card["City"]
            County = card["County"]
            if County:
                address = f"{Street}, {Section}, {City}, {County}, Poland"
            else:
                address = f"{Street}, {Section}, {City}, Poland"

            dataUsage = {
                "position": position,
                "card_url": card_url,
                "external_id": ID,
                "title": Title,
                "rent": rent,
                "square_meters": square_meters,
                "address": address,
                
                "street": Street,
                "section": Section,
                "city": City,
                "county": County,
                **response.meta
            }

            maxonSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, headers=self.headers, dont_filter=True, meta=dataUsage)

            
        if len(cards) > 0:
                
            prev_page = int(parse_qs(response.url)['Page'][0])
            next_page = int(parse_qs(response.url)['Page'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&Page={prev_page}",f"&Page={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True, meta=response.meta)




    def parseApartment(self, response):
    
        rent = response.meta['rent']
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
        else:
            rent = None
            return

        currency = "PLN"

        property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.meta['external_id']

        square_meters = response.meta['square_meters']
        if square_meters:
            square_meters = square_meters.split(",")[0]


        room_count = response.css("span.header:contains('liczba pokoi:') + span.value::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")

        bathroom_count = response.css(".tab-content #params .l:contains('Liczba łazienek') + .h::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count).replace(".", "")

        description = response.css(".tab-content #home ::text").getall()
        description = " ".join(description)
        if description:
            description = remove_white_spaces(description)
        else:
            description = None

        city = response.meta['city']
        address = response.meta['address']
        title = response.meta['title']
        zipcode = None
        
        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            script_map = remove_white_spaces(script_map)            
            pattern = re.compile(r'var myLatLng = { lat: (\d*\.?\d*) , lng: (\d*\.?\d*) };')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            zipcode = None


        images = response.css('#top_photos .item a::attr(href)').getall()
        external_images_count = len(images)

        floor = response.css("span.header:contains('piętro:') + span.value::text").get()
        if floor:
            floor = remove_white_spaces(floor)
            floor = extract_number_only(floor).replace(".", "")


        furnished = response.css(".tab-content #params .l:contains('Meble') + .h i::attr(class)").get()
        if furnished:
            if "fa-check-square" in furnished:
                furnished = True
            else:
                furnished = False
        else:
            furnished = False
        
        elevator = response.css(".tab-content #params .l:contains('Winda') + .h i::attr(class)").get()
        if elevator:
            if "fa-check-square" in elevator:
                elevator = True
            else:
                elevator = False
        else:
            elevator = False

        balcony = response.css(".tab-content #params .l:contains('Liczba balkonów') + .h::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False

        terrace = response.css(".tab-content #params .l:contains('Liczba tarasów') + .h::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False
        
        parking = response.css(".tab-content #params .l:contains('Garaż') + .h::text, .tab-content #params .l:contains('Parking naziemny') + .h::text").get()
        if parking:
            parking = True
        else:
            parking = False

        landlord_name = response.css(".agent_details .name::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
        
        landlord_phone = response.css(".agent_details .contact_item div + a::attr(href)").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","").replace("tel:","")
            
        landlord_email = "kontakt@maxon.pl"
        
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
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
