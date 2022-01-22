import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
import math
from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json

class remaxcondosplusSpider(scrapy.Spider):

    name = 'remaxcondosplus'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['remaxcondosplus.com']
    start_urls = ["https://remaxcondosplus.com/WebService.svc/SearchListingsAdapter?fwdId=59f8bcc96ce6b51170e9c2d3&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.65771%2C%22Longitude%22%3A-79.38618%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D"]

    position = 1

    def parse(self, response):

        apartments = json.loads(str(response.text))['results']
        for apartment in apartments:
            if 'mlNum' in apartment:
                external_id = str(apartment['listingId'])
                print('='*50)
                print(external_id)
                print('='*50)
                
                external_link = 'https://remaxcondosplus.com/Listing/'+str(apartment['mlNum'])+'?id='+str(apartment['listingId'])
                
                
                
                title = apartment['address']
                city = apartment['addressDetails']['city']

                zipcode = apartment['addressDetails']['zip']

                address = str(apartment['address']).replace('#Parking - ','')


                
                latitude = str(apartment['latitude'])
                longitude = str(apartment['longitude'])

                property_type = apartment['propertyTypeId']
                if property_type=='Condo' or property_type=='Apartment':
                    property_type = 'apartment'
                else:
                    property_type = 'house'

                images = []

                pics = apartment['images']
                for pic in pics:
                    images.append('https://cflare.smarteragent.com/rest/Resizer?url={img}'.format(img=pic))
                square_meters = 0
                if 'sqft' in apartment:
                    n1 = int(apartment['sqft'].split('-')[0])
                    n2 = int(apartment['sqft'].split('-')[1])
                    if n1==0:
                        square_meters = sq_feet_to_meters(n2)
                    else:
                        square_meters = sq_feet_to_meters(n1+2)

                else:
                    square_meters=0

                room_count = int(apartment['beds'])
                if 'bedsPlus' in apartment:
                    room_count+=int(apartment['bedsPlus'])
                if not room_count or len(str(room_count))==0:
                    room_count='1'
                bathroom_count = int(apartment['baths'])
                print(square_meters)

                rent= int(apartment['listPrice'])
                currency='CAD'

                
                parking= True if int(apartment['parkingSpaces'])>0 else False
                description = apartment['description']
                swimming_pool = True if 'pool' in description else False

                '''dataUsage = {
                    "position": self.position,
                    "property_type": property_type,
                    "title":title,
                    "external_id": external_id,
                    "external_link": external_link,
                    "city": city,
                    "address":address,
                    "zipcode":zipcode,
                    "longitude": longitude,
                    "latitude": latitude,
                    "description":description,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "images":images,
                    "bathroom_count": bathroom_count,
                    #"terrace": terrace,
                    "parking": parking,
                    "rent": rent,
                    "currency": currency,
                }'''

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
                item_loader.add_value("external_images_count", len(images))

                
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", currency)

                
                item_loader.add_value("swimming_pool", swimming_pool)
                #item_loader.add_value("furnished", response.meta['furnished'])
                #item_loader.add_value("floor", floor)
                item_loader.add_value("parking", parking)

                
                #item_loader.add_value("elevator", response.meta['elevator'])
                #item_loader.add_value("balcony", response.meta['balcony'])
                #item_loader.add_value("terrace", response.meta['terrace'])
                item_loader.add_value("landlord_name",'RE/MAX Condos Plus Corp. Brokerage')
                #item_loader.add_value("landlord_email", response.landlord_email)
                item_loader.add_value("landlord_phone", '416-203-6636')
                item_loader.add_value("position", self.position)

                yield item_loader.load_item()
                self.position += 1

    def parseApartment(self,response):
        pass