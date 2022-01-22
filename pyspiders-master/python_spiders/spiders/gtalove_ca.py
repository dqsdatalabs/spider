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

class GtaloveSpider(scrapy.Spider):
        
    name = 'gtalove_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['gtalove.com']
    
    position = 1

    def start_requests(self):
        start_urls = [
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=toronto&pro=houses&type=rent&show=_a',
            'city': 'Toronto',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=toronto&pro=condos&type=rent&show=_a',
            'city': 'Toronto',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=brampton&pro=houses&type=rent&show=_a',
            'city': 'Brampton',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=brampton&pro=condos&type=rent&show=_a',
            'city': 'Brampton',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=mississauga&pro=houses&type=rent&show=_a',
            'city': 'Mississauga',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=mississauga&pro=condos&type=rent&show=_a',
            'city': 'Mississauga',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=burlington&pro=houses&type=rent&show=_a',
            'city': 'Burlington',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=burlington&pro=condos&type=rent&show=_a',
            'city': 'Burlington',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=oakville&pro=houses&type=rent&show=_a',
            'city': 'Oakville',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=oakville&pro=condos&type=rent&show=_a',
            'city': 'Oakville',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=hamilton&pro=houses&type=rent&show=_a',
            'city': 'Hamilton',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=hamilton&pro=condos&type=rent&show=_a',
            'city': 'Hamilton',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=markham&pro=houses&type=rent&show=_a',
            'city': 'Markham',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=markham&pro=condos&type=rent&show=_a',
            'city': 'Markham',
            'property_type': 'apartment',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=richmond-hill&pro=houses&type=rent&show=_a',
            'city': 'Richmond Hill',
            'property_type': 'house',
            },
            {'url': 'https://test-api.gtalove.com/api/list-listings?city=richmond-hill&pro=condos&type=rent&show=_a',
            'city': 'Richmond Hill',
            'property_type': 'apartment',
            },
        ]
                
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta = url)

    def parse(self, response):

        jsonResponse = response.json()
        for index, card in enumerate(jsonResponse):
            
            if not card["Lp_dol"] or card["Lp_dol"] == "":
                continue

            position = self.position
            card_url = f"https://test-api.gtalove.com/api/get-single-listing/{card['Ml_num']}/_a"
               
            dataUsage = {
                "position": position,
                "card_url": card_url,
                **response.meta
            }
                    
            
            GtaloveSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)


    def parseApartment(self, response):
    
        jsonResponse = response.json()
        
        external_id = jsonResponse['Ml_num']
        property_type = response.meta['property_type']
        
        
        Municipality = jsonResponse['Municipality']
        Addr = jsonResponse['Addr'].replace(" ","-")
        Ml_num = jsonResponse['Ml_num']
        external_link = f"https://www.gtalove.com/real-estate/{Municipality}/{Addr}/{Ml_num}"
        
        rent = jsonResponse['Lp_dol']
        if rent:
            rent = rent.split(".")[0]
        else:
            rent = None
            
        currency = "CAD"
        
        room_count = jsonResponse['Rms']
        if not room_count or room_count == "":
            room_count = jsonResponse['Br']

        bathroom_count = jsonResponse['Bath_tot']
           
        address = f"{jsonResponse['Addr']}, {jsonResponse['Municipality']}, {jsonResponse['Zip']}"
        title = f"{property_type} - {address} - {Ml_num}"
        
        city = jsonResponse['Municipality']
        zipcode = jsonResponse['Zip']
        latitude = str(jsonResponse['lat'])
        longitude = str(jsonResponse['lng'])

        description = jsonResponse['Ad_text'] + jsonResponse['Extras']
        
        
        square_meters = description.lower()
        if square_meters:
            pattern = re.compile(r'(\d+)?\s?sq\.?\s?(ft|feet)?\.?', re.IGNORECASE)
            data_from_regex = pattern.search(square_meters)
            if data_from_regex:
                square_meters = data_from_regex.group(1)
                if square_meters: 
                    square_meters = sq_feet_to_meters(square_meters)
            else:
                square_meters = None
        
        
        
        
        
        external_images_count = jsonResponse['img_count']
        try:
            if external_images_count > 0:
                images_link = f"https://test-api.gtalove.com/listing_images/{jsonResponse['Ml_num']}/"
                response_images_data = requests.get(images_link)
                images_data = Selector(text=response_images_data.text)
            
                images = images_data.css("td a:contains('jpg')::attr(href),td a:contains('png')::attr(href),td a:contains('jpeg')::attr(href)").getall()
                images = [f"https://test-api.gtalove.com/listing_images/{jsonResponse['Ml_num']}/{img}" for img in images]
            else:
                images = []        
        except Exception as err:
            pass
        
        
        balcony = "balcony" in description.lower()
        if balcony:
            balcony = True
        else:
            balcony = False 
        
        terrace = "terrace" in description.lower()
        if terrace:
            terrace = True
        else:
            terrace = False  
        
        
        pets_allowed = jsonResponse['Pets']
        if pets_allowed and pets_allowed != "N" and pets_allowed != "None":
            pets_allowed = True     
        else:
            pets_allowed = False   
            
        elevator = jsonResponse['Elevator']
        if elevator != "None" and elevator != "" and elevator != "N":
            elevator = True
        else:
            elevator = False 

        parking = jsonResponse['Prkg_inc']
        if parking == "Y":
            parking = True
        else:
            parking = False  
           
        swimming_pool = jsonResponse['Pool']
        if swimming_pool != "None" and swimming_pool != "":
            swimming_pool = True
        else:
            swimming_pool = False  
        
        furnished = jsonResponse['Furnished']
        if furnished == "Y" or furnished == "Part":
            furnished = True 
        else:
            furnished = False    


        washing_machine = jsonResponse['Laundry']
        if washing_machine == "Ensuite":
            washing_machine = True
        else:
            washing_machine = False 

        dishwasher = "dishwasher" in description.lower()
        if dishwasher:
            dishwasher = True 
        else:
            dishwasher = False 
        
        landlord_phone = "4163004674"
        landlord_email = "info@gtalove.com"
        landlord_name = "gtalove agency"


        if rent and rent >= 100: 
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", external_link)
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
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
