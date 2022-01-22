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

class IconacasaSpider(scrapy.Spider):
        
    name = 'iconacasa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.iconacasa.com']
    start_urls = ['https://www.iconacasa.com/index.php/opportunita']
    post_url = "https://www.iconacasa.com/index.php?option=com_iproperty&task=ajax.ajaxSearchCustom&format=raw&&lat=41.84273602061836&lng=12.608243845200175&filter_stype=4&filter_price_low=&filter_price_high=&filter_beds=&filter_cat=1&filter_keyword=&filter_listing_office=&"

    position = 1

    def parse(self, response):
        
        csrf_token_script = response.css(".joomla-script-options.new::text").get()
        if csrf_token_script:
            pattern = re.compile(r'"csrf.token":"(\w*)"')
            x = pattern.search(csrf_token_script)
            csrf_token = x.groups()[0]

        
        self.post_url = f"{self.post_url}{csrf_token}=1"

        yield Request(self.post_url,
            callback=self.parse2,
            dont_filter=True)



    def parse2(self, response):
        
        jsonResponse = response.json()

                
        if len(jsonResponse['data']) > 0:
            for index, card in enumerate(jsonResponse['data']):

                position = self.position
                
                property_type = "apartment"
                
                
                external_link = "https://www.iconacasa.com/index.php/opportunita/property/" + card['id'] + "-" + card['alias']
                
                
                longitude = card['longitude']
                latitude = card['latitude']
                
                square_meters = int(card['sqft'])
                
                room_count = int(card['beds'])
                if room_count == 0:
                    room_count = 1
                    
                bathroom_count = int(card['baths'].split(".")[0])
                if bathroom_count == 0:
                    bathroom_count = 1
                
                title = card['title']
                
                rent = int(card['fprice'])
                currency = "EUR"
                
                
                street_num = card['street_num']
                street = card['street']
                city = card['city']
                region = card['region']
                county = card['county']
                
                address = f"{county}-{region}-{city}-{street}-{street_num}"

                
                landlord_name = "Iconacasa " + card['lname']
                landlord_phone = card['phone'].replace(" ","").replace(".","")
                landlord_email = card['lname'].lower().replace(" ","") + "@iconacasa.com"

                
                dataUsage = {
                    "position": position,
                    "property_type": property_type,
                    "external_link": external_link,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "title": title,
                    "rent": rent,
                    "currency": currency,
                    "city": city,
                    "address": address,
                    "landlord_name": landlord_name,
                    "landlord_phone": landlord_phone,
                    "landlord_email": landlord_email,
                }
                
                
                IconacasaSpider.position += 1
                yield Request(external_link, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
      





    def parseApartment(self, response):     

        external_id = response.css(".profile_data ul li:contains('Rif')::text").getall()
        external_id = " ".join(external_id)
        if external_id:
            external_id = remove_white_spaces(external_id)
        
    
        description = response.css('.property-text p::text, .property-text ol li::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)
        
        images = response.css('.carousel-inner img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)

        energy_label = response.css(".info-list li:contains('energetica') strong::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)
        
        
        utilities = response.css(".info-list ul li:contains('Spese') strong::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)
        
        floor = response.css(".profile_data ul li:contains('Piano') strong::text").get()
        if floor:
            floor = remove_white_spaces(floor)
        
        
        elevator = response.css(".profile_data ul li:contains('Ascensore') strong::text").get()
        if elevator:
            elevator = remove_white_spaces(elevator).lower()
            if elevator == "si":
                    elevator = True
            elif elevator == "no":
                elevator = False
            else:
                elevator = False 
        
        parking = response.css(".profile_data ul li:contains('Posto') strong::text").get()
        if parking:
            parking = remove_white_spaces(parking).lower()
            if parking == "si":
                    parking = True
            elif parking == "no":
                parking = False
            else:
                parking = False 
            
        
        if int(response.meta['rent']) > 0:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", response.meta['title'])
            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", response.meta['square_meters'])
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", int(response.meta['rent']))
            item_loader.add_value("currency", response.meta['currency'])
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("landlord_name", response.meta['landlord_name'])
            item_loader.add_value("landlord_email", response.meta['landlord_email'])
            item_loader.add_value("landlord_phone", response.meta['landlord_phone'])
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
