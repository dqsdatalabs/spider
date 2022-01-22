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


class immoblySpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'immobly_{locale}'
    external_source = f"immobly_PySpider_{country}_{locale}"
    allowed_domains = ['immobly.com']

    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://immobly.com/advanced-search/?keyword_search=&filter_search_type%5B%5D=&filter_search_action%5B%5D=rent&submit=Search&is11=11&advanced_country=&advanced_contystate=&advanced_city=&advanced_area=&status=&bedrooms=&bathrooms=&property-id=&min-size-m2=&max-size-m2=&min-price=&max-price=&wpestate_regular_search_nonce=17a030f673&_wp_http_referer=%2F',
                'property_type': 'apartment',
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)

    def parse(self, response):

        cards = response.css("#listing_ajax_container .listing_wrapper")


        for index, card in enumerate(cards):


            position = self.position

            card_url = card.css("::attr(data-modal-link)").get()



            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }

            immoblySpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        if len(cards) > 0:       
            nextPageUrl = response.css(".roundright a::attr(href)").get()
            if nextPageUrl:
                nextPageUrl = response.urljoin(nextPageUrl)

            if nextPageUrl and nextPageUrl != response.url:
                yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta=response.meta)


    def parseApartment(self, response):
        
        rent = response.css("#accordion_prop_details .listing_detail:contains('Price')::text").get()
        if rent:
            rent = remove_white_spaces(rent)
            rent = extract_number_only(rent)
            rent = str(rent).split(".")[0]
        else:
            rent = None
            return

        currency = "EUR"

        type = response.css(".property_title_label.actioncat ::text").get()
        if type and type =="Houses":
            property_type = "house"
        else:        
            property_type = response.meta['property_type']

        position = response.meta['position']

        external_id = response.css("#accordion_prop_details .listing_detail:contains('Property Id')::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)


        square_meters = response.css("#accordion_prop_details .listing_detail:contains('Property Size')::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters).split(".")[0]
            square_meters = extract_number_only(square_meters)

        room_count = response.css("#accordion_prop_details .listing_detail:contains('Bedrooms')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
            if room_count :
                room_count = room_count.replace(".", "")
                

        bathroom_count = response.css("#accordion_prop_details .listing_detail:contains('Bathrooms')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
            if bathroom_count :
                bathroom_count = bathroom_count.replace(".", "")


        description = response.css("#wpestate_property_description_section > p::text").getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)
        else:
            description = None

        title = response.css("h1.entry-title.entry-prop::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)
        
        
        
        


        address = response.css("div.property_categs ::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)
            
        city = response.css("#accordion_prop_addr .listing_detail:contains('City') a::text").get()
        if city:
            city = remove_white_spaces(city)
            
        zipcode = response.css("#accordion_prop_addr .listing_detail:contains('Zip')::text").get()
        if zipcode:
            zipcode = remove_white_spaces(zipcode)
            
            
        longitude = response.css("#gmap_wrapper::attr('data-cur_long')").get()
        latitude = response.css("#gmap_wrapper::attr('data-cur_lat')").get()

        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                address = responseGeocodeData['address']['LongLabel']
                city = responseGeocodeData['address']['City']
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            pass

        images = response.css('#property_slider_carousel .item  a::attr(href)').getall()
        external_images_count = len(images)



        energy_label = response.css("#accordion_prop_details .listing_detail:contains('Energy class')::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)

        
        landlord_name = response.css("#primary h4 a::text").getall()
        if landlord_name:
            landlord_name = " ".join(landlord_name)
            landlord_name = remove_white_spaces(landlord_name)

        landlord_phone = response.css(".agent_detail.agent_phone_class a::text").getall()
        if landlord_phone:
            landlord_phone = " ".join(landlord_phone)
            landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")
        
        landlord_email = response.css(".agent_detail.agent_email_class a::text").getall()
        if landlord_email:
            landlord_email = " ".join(landlord_email)
            landlord_email = remove_white_spaces(landlord_email).replace(" ","")

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
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
