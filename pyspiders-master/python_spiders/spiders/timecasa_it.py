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

class TimecasaSpider(scrapy.Spider):

    name = 'timecasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.timecasa.it']
    post_url = "https://www.timecasa.it/wp-admin/admin-ajax.php?search_location=&lat=&lng=&use_radius=on&search_radius=50&location%5B%5D=&status%5B%5D=affitto&type%5B%5D=appartamento&type%5B%5D=appartavilla&type%5B%5D=attico&type%5B%5D=bifamiliare&type%5B%5D=casa&type%5B%5D=casa-semindipendente&type%5B%5D=casa-singola&type%5B%5D=doppia&type%5B%5D=mansarda&type%5B%5D=palmento&type%5B%5D=rustico-casale-cascina&type%5B%5D=villa-2&type%5B%5D=villa-a-schiera&bedrooms=&bathrooms=&min-area=&max-area=&property_id=&min-price=200&max-price=2500000&search_args=YTo2OntzOjk6InBvc3RfdHlwZSI7czo4OiJwcm9wZXJ0eSI7czoxNDoicG9zdHNfcGVyX3BhZ2UiO3M6MjoiMTYiO3M6NToicGFnZWQiO3M6MToiMCI7czoxMToicG9zdF9zdGF0dXMiO3M6NzoicHVibGlzaCI7czo5OiJ0YXhfcXVlcnkiO2E6Mzp7aTowO2E6Mzp7czo4OiJ0YXhvbm9teSI7czoxNToicHJvcGVydHlfc3RhdHVzIjtzOjU6ImZpZWxkIjtzOjQ6InNsdWciO3M6NToidGVybXMiO2E6MTp7aTowO3M6NzoiYWZmaXR0byI7fX1pOjE7YTozOntzOjg6InRheG9ub215IjtzOjEzOiJwcm9wZXJ0eV90eXBlIjtzOjU6ImZpZWxkIjtzOjQ6InNsdWciO3M6NToidGVybXMiO2E6MTM6e2k6MDtzOjEyOiJhcHBhcnRhbWVudG8iO2k6MTtzOjEyOiJhcHBhcnRhdmlsbGEiO2k6MjtzOjY6ImF0dGljbyI7aTozO3M6MTE6ImJpZmFtaWxpYXJlIjtpOjQ7czo0OiJjYXNhIjtpOjU7czoyMDoiY2FzYS1zZW1pbmRpcGVuZGVudGUiO2k6NjtzOjEyOiJjYXNhLXNpbmdvbGEiO2k6NztzOjY6ImRvcHBpYSI7aTo4O3M6ODoibWFuc2FyZGEiO2k6OTtzOjg6InBhbG1lbnRvIjtpOjEwO3M6MjI6InJ1c3RpY28tY2FzYWxlLWNhc2NpbmEiO2k6MTE7czo3OiJ2aWxsYS0yIjtpOjEyO3M6MTU6InZpbGxhLWEtc2NoaWVyYSI7fX1zOjg6InJlbGF0aW9uIjtzOjM6IkFORCI7fXM6MTA6Im1ldGFfcXVlcnkiO2E6Mzp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDtzOjA6IiI7aToxO2E6Mjp7czo4OiJyZWxhdGlvbiI7czozOiJBTkQiO2k6MDthOjE6e2k6MDthOjQ6e3M6Mzoia2V5IjtzOjE5OiJmYXZlX3Byb3BlcnR5X3ByaWNlIjtzOjU6InZhbHVlIjthOjI6e2k6MDtkOjIwMDtpOjE7ZDoyNTAwMDAwO31zOjQ6InR5cGUiO3M6NzoiTlVNRVJJQyI7czo3OiJjb21wYXJlIjtzOjc6IkJFVFdFRU4iO319fX19&search_URI=status%255B%255D%3Daffitto%26keyword%3D%26states%255B%255D%3Dacireale%26rooms%3D%26max-price%3D%26min-price%3D&search_geolocation=&houzez_save_search_ajax=1fd105e733&action=houzez_half_map_listings&paged=1&sortby=&item_layout=v1"

    position = 1

    def start_requests(self):
        yield Request(
            url = self.post_url,
            method = 'GET', 
            callback = self.parse,
            dont_filter = True
        )

    def parse(self, response):


        jsonResponse = response.json()
        

        cards = jsonResponse['properties'] if 'properties' in jsonResponse else []

        for index, card in enumerate(cards):

            position = self.position

            property_type = "apartment"

            card_url = card["url"]

            title = card["title"]

            latitude = card["lat"]
            longitude = card["lng"]                 
            address = card['address']
            
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
                
            rent = card['pricePin']
            if rent:
                rent = extract_number_only(rent).replace(".","")
            else:
                rent = None
                
            currency = card['pricePin']
            if currency:
                currency = currency_parser(currency, self.external_source)
            else:
                currency = "EUR"
                        
            
            
            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
                "title": title,
                "latitude": latitude,
                "longitude": longitude,
                "zipcode": zipcode,
                "address": address,
                "city": city,
                "rent": rent,
                "currency": currency,
                
            }

            TimecasaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        if len(cards) > 0:
            
            prev_page = int(parse_qs(response.url)['paged'][0])
            next_page = int(parse_qs(response.url)['paged'][0]) + 1
            parsed = urlparse(response.url)
            new_query = parsed.query.replace(f"&paged={prev_page}",f"&paged={next_page}")
            parsed = parsed._replace(query= new_query)
            nextPageUrl = urlunparse(parsed)
            
            if nextPageUrl:
                yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)



    def parseApartment(self, response):

        external_id = response.css("strong:contains('ID proprietà:') + span::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)

        square_meters = response.css("strong:contains('Dimensioni proprietà:') + span::text,strong:contains('Area di atterraggio:') + span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css("strong:contains('Stanze:') + span::text,strong:contains('Stanza:') + span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = response.css("strong:contains('Stanza da letto:') + span::text,strong:contains('Stanze da letto:') + span::text").get()
            if room_count:
                room_count = remove_white_spaces(room_count)
                room_count = extract_number_only(room_count)
            else: 
                room_count = 1

        bathroom_count = response.css("strong:contains('Stanza da bagno:') + span::text,strong:contains('Stanze da bagno:') + span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)  
        # else:
        #     bathroom_count = 1             
    
        description = response.css('#property-description-wrap .block-content-wrap p::text,#property-description-wrap .block-content-wrap p strong::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('#lightbox-slider-js img::attr(src)').getall()
        external_images_count = len(images)
        

        
        furnished = response.css("#property-features-wrap .block-content-wrap a:contains('Arredato')::text, #property-features-wrap .block-content-wrap a:contains('Parzialmente arredato')::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False  
              
        balcony = response.css("#property-features-wrap .block-content-wrap a:contains('Balconi')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False      
        
        terrace = response.css("#property-features-wrap .block-content-wrap a:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False      
        
        parking = response.css("#property-features-wrap .block-content-wrap a:contains('Posto auto')::text").get()
        if parking:
            parking = True
        else:
            parking = False      
        
        
        landlord_email = response.css("#property-contact-agent-wrap input[name='target_email']::attr(value)").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email)
        else:
            landlord_email = "info@timecasa.com"
        
        landlord_phone = response.css("#property-contact-agent-wrap ul.agent-information .agent-phone  a::text").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone)
        else:
            landlord_phone = "800212250"
        
        landlord_name = response.css("#property-contact-agent-wrap ul.agent-information .agent-name::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)
        else:
            landlord_name = "Time Network Immobiliare"

        if response.meta['rent']: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title",  response.meta['title'])
            item_loader.add_value("description", description)
            item_loader.add_value("city",  response.meta['city'])
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value("address",  response.meta['address'])
            item_loader.add_value("latitude",  response.meta['latitude'])
            item_loader.add_value("longitude",  response.meta['longitude'])
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", response.meta['currency'])
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
