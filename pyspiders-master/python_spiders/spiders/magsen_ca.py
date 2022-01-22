from operator import le
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
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
import math

class Magsen_Spider(scrapy.Spider):

    name = 'magsen'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['812b4ba287f5ee0bc9d43bbf5bbe87fb.vancouver.bc.mygoodreal.net']
    #start_urls = ['https://www.magsen.ca/en/listings/properties-for-rent/properties-for-rent']

    position = 1


    def start_requests(self):

        start_urls = requests.get('https://812b4ba287f5ee0bc9d43bbf5bbe87fb.vancouver.bc.mygoodreal.net/api/en/residential/fixList?limit=12&filter=undefined&theme=c&type=0&layout=grid&list_id=5&each_row=4&order=3&listing=12&color=%2308255c&ShowBrokerInformation=true&is_image=0&style=light&page=1&iframe_user_id=&_=1634719892488')
        result = json.loads(start_urls.text)

        apartments = result['data']
        
        for apartment in apartments:
            detailsID = str(apartment['mls_id'])
            external_id = str(apartment['refernce_id'])
            url = "https://812b4ba287f5ee0bc9d43bbf5bbe87fb.vancouver.bc.mygoodreal.net/en/residential_exclusive/detail/"+detailsID+"?theme=c&ShowBrokerInformation=true&is_image=0&style=light&color=%2308255c"
            external_link = url
            title = apartment['title']
            description = apartment['public_remarks']
            address = apartment['address']
            city = apartment['city']
            zipcode = apartment['postal_code']
            latitude=apartment['latitude']
            longitude=apartment['longitude']
            landlord_phone=apartment['contact']
            rent = int(float(apartment['monthly_price']))
            available_date = apartment['date_available']
            washing_machine = True if 'washer' in description else False
            pets_allowed = False if 'No PETS' in description else True

            property_type = 'house' if 'House' in apartment['dwelling_type'] else 'apartment'
            
            square_meters = int(float(apartment['floor_area'])/10.764)+1

            dataUsage = {
                "property_type": property_type,
                'title':title,
                "external_id": external_id,
                "external_link": external_link,
                "landlord_phone":landlord_phone,
                "city": city,
                'description':description,
                "address":address,
                "zipcode":zipcode,
                "washing_machine":washing_machine,
                "longitude": longitude,
                "latitude": latitude,
                "square_meters": square_meters,
                "available_date": available_date,
                "pets_allowed": pets_allowed,
                "rent": rent
                }


            yield Request(external_link, meta=dataUsage,
                callback=self.parseApartment)
        
    def parseApartment(self,response):
        

        details = response.css(".pc_res_imgbottom .d-flex *:not(br)::text").getall()
        for i,info in enumerate(details):
            if 'Bedroom' in info:
                room_count = [int(s) for s in details[i+1].split() if s.isdigit()][0]
            if 'Bathroom' in info:
                bathroom_count = [int(s) for s in details[i+1].split() if s.isdigit()][0]
            if 'Parking' in info:
                parking = True if [int(s) for s in details[i+1].split() if s.isdigit()][0]>0 else False
            

        images = response.css(".slides.mobile_slides_big .img-fluid::attr(src)").getall()


        
        if response.meta['rent']>0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta["external_id"])
            item_loader.add_value("title", response.meta["title"])
            item_loader.add_value("description", response.meta["description"])
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", int(int(response.meta['square_meters'])*10.764))
            item_loader.add_value('parking',parking)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", "CAD")
            item_loader.add_value("available_date", response.meta['available_date'])
            item_loader.add_value("washing_machine", response.meta['washing_machine'])
            item_loader.add_value("pets_allowed", response.meta['pets_allowed'])
            item_loader.add_value("landlord_name", 'Magsen Realty Inc.')
            item_loader.add_value("landlord_email", 'info@magsen.ca')
            item_loader.add_value("landlord_phone", response.meta['landlord_phone'])
            item_loader.add_value("position", self.position)

            self.position+=1
            yield item_loader.load_item()