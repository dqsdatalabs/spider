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

class terrequitySpider(scrapy.Spider):

    name = 'terrequity'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['terrequity.com']
    start_urls = ['https://www.terrequity.com/ajax/dla-properties.php?action=get_properties_from_map']

    position = 1

    def parse(self, response):


        body = {
            'action':'get_properties_from_map',
            'latMin':'42.52668504943296',
            'lngMin':'-140.28265102743808',
            'latMax':"68.12550847162213",
            'lngMax':"-58.36858852743807",
            'startPos':"0",
            'saleRent':'Rent'
            #'startPos':'72'
        }
        
       
        for i in range(0,200,12):

            body['startPos']=str(i)
            yield FormRequest(url = self.start_urls[0],
                formdata=body,
                method='POST',
                callback=self.parseApartment
            )


    def parseApartment(self, response):
        

        apartments = json.loads(response.text)['body']['Properties']
        if len(apartments)!=0:
            for apartment in apartments:
                

                external_link = 'https://www.terrequity.com/dla-prop-detail.php?ml_num='+apartment['id']
                external_id = apartment['id']
                

                latitude = str(apartment['lat'])
                longitude = str(apartment['lon'])
                address = apartment['address']

                if latitude != "0":
                    responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                    responseGeocodeData = responseGeocode.json()
                    zipcode = responseGeocodeData['address']['Postal']
                    city = responseGeocodeData['address']['City']    
                else:
                    responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                    responseGeocodeData = responseGeocode.json()

                
                    responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")

                    responseGeocodeData = responseGeocode.json()
                    zipcode = responseGeocodeData['address']['Postal']
                    city = responseGeocodeData['address']['City']   
                

                txt = apartment['size']
                rex = re.findall(r'\d+',txt)
                square_meters = 0
                try:
                    if len(rex)>1:
                        if int(rex[0])==0:
                            total = ((int(rex[1])))/10.7639
                        else:
                            total = ((int(rex[0])+int(rex[1]))/2)/10.7639
                    else:
                        total = ((int(rex[0])))/10.7639
                    square_meters = int(total)
                except:
                    pass
                
               
                room_count = apartment['beds']
                if len(room_count) == 0 or not room_count or room_count=='0':
                    room_count="1"
                bathroom_count = apartment['bath']
                if bathroom_count == "":
                    bathroom_count="1"

                rent=apartment['price'].replace('.00','')
            
                currency='CAD'
                parking= True if apartment['parking'] =='1' else False
                

                dataUsage = {
                    "external_id": external_id,
                    "external_link": external_link,
                    "city": city,
                    "address":address,
                    "zipcode":zipcode,
                    "longitude": longitude,
                    "latitude": latitude,
                    "square_meters": square_meters,
                    "room_count": room_count,
                    "bathroom_count": bathroom_count,
                    "parking": parking,
                    "rent": rent,
                    "currency": currency,
                }

                body = {
                    'ml_num':'W5369412'
                }
                body['ml_num']=external_id

            
                yield FormRequest(url = external_link,
                    formdata=body,
                    method='POST',meta=dataUsage,
                    callback=self.parse_description
                )

            


    def parse_description(self,response):

        property_type='apartment'
        if 'Commercial' not in response.css('.d-text::text').getall():

            zipcode = response.meta['zipcode']
            if 'E5434391' in response.url:
                print(zipcode)
            if zipcode=='' or not zipcode or len(str(zipcode))<3:
                zipcode = response.css(".data-list *:contains('Postal Code') .d-text::text").get()
            zipcode = response.css(".data-list *:contains('Postal Code') .d-text::text").get()
            description = remove_white_spaces(response.css('.desc::text').get())

            images = response.css('.fancybox-thumb::attr(href)').getall()
            available_date=response.css(".data-list *:contains('Possession Date') .d-text::text").get()
            for i in range(0,len(images)):
                images[i] = 'https://www.terrequity.com/'+images[i]
            
            

            title = response.meta['address']
            
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta["external_id"])
            item_loader.add_value("title", title)

            
            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])
            
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", int(int(response.meta['square_meters'])*10.764))
            item_loader.add_value("room_count", response.meta['room_count'])
            item_loader.add_value("bathroom_count", response.meta['bathroom_count'])
            

            
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            
            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", response.meta['currency'])

            
            item_loader.add_value('balcony', 'Balcon' in description)
            item_loader.add_value("furnished", 'Furnished' in description)
            item_loader.add_value("washing_machine", 'Laundry' in description)
            item_loader.add_value("swimming_pool", 'Pool' in description)
            item_loader.add_value("parking", response.meta['parking'])

            
            item_loader.add_value("elevator", 'levator' in description)
            item_loader.add_value('dishwasher', 'Dishwasher' in description)
            item_loader.add_value("available_date",available_date)
            item_loader.add_value("terrace", 'Terrace' in description)
            item_loader.add_value("landlord_name", 'ROYAL LEPAGE TERREQUITY REALTY, BROKERAGE')
            item_loader.add_value("landlord_email", 'info@terrequity.com')
            item_loader.add_value("landlord_phone", '416-496-9220')
            item_loader.add_value("position", self.position)

            self.position += 1
            yield item_loader.load_item()