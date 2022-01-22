from requests.api import post
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

class Eynard_immobiliareSpider(scrapy.Spider):

    name = 'eynard_immobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['eynard-immobiliare.it']
    start_urls = ['https://eynard-immobiliare.it/advanced-search/?advanced_area=&filter_search_type%5B%5D=&filter_search_action%5B%5D=affitto&id=&price_low=0&price_max=3000000&submit=Cerca&wpestate_regular_search_nonce=28da75ab8c&_wp_http_referer=%2Fproperty_category%2Fappartamento%2F']

    position = 1
    def parse(self, response):

        apartments = response.css(".listing-unit-img-wrapper a::attr(href)").getall()

        for apartment in apartments:
            yield Request(apartment,callback=self.parseDetails)
        

    def parseDetails(self,response):
        print('='*50)
        print(response.url)
        print('='*50)
        
        title = response.css(".entry-title.entry-prop *::text").get()

        #description
        descriptionText = response.css("#description *::text").getall()
        description = ''
        for text in descriptionText:
            description+=text.replace('\n,\t',"")
        description =  remove_white_spaces(description)

        strID = re.findall(r'ID: \d+',description)
        external_id=""
        if len(strID)>0:
            external_id = remove_white_spaces(response.css(".listing_detail.col-md-4:contains('ID')::Text").get())
        
        addressList = response.css(".property_categs *::text").getall()
        address = ''.join(addressList[-4:-1])
     


        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        longitude  = str(longitude)
        latitude  = str(latitude)

  

        Rooms_Baths_sq = str(response.css(".single-overview-section *::text"))
        rex =  re.findall(r'\'\d{3}\'|\'\d{2}\'|\d Camere|\d Bagni',Rooms_Baths_sq)
        room_count = 1
        bathroom_count=1
        square_meters=0
        for i in rex:
            if 'Camere' in i:
                room_count=int(i[0])
            if 'Bagni' in i:
                bathroom_count=int(i[0])
            if '\'' in i:
                square_meters=int(rex[-1].replace('\'',""))

        
        rent = 0
        price = str(response.xpath("//*[@class=\"price_area\"]/text()").get())
        price = price.replace(' €',"").replace('.','')
        if 'None' in price:
            rent=0
        else:
            rent = int(price)

        #Furnished
        furnishedText = str(response.css("#features *::text"))
        check_Furnished = re.findall(r'Arredato: .\'',furnishedText)
        furnished = False
        if len(check_Furnished)>0:
            furnished = True if "S" in check_Furnished[0] else False
        
        detailsText = response.css("#details *::text").getall()
        text = "".join(detailsText)
        rex = re.findall(r'energetica : (.)',text)
        energy_label = ''
        if len(rex)>0:
            energy_label = rex[0]


        images = response.css(".carousel-indicators.carousel-indicators-classic  a img::attr(src)").getall()
        
        
        for i in range(0,len(images)):
            rex = re.findall(r'\-\d+x\d+',images[i])
            if len(rex)>1:
                images[i] = images[i].replace(str(rex[1]),"")
            else:
                images[i] = images[i].replace(str(rex[0]),"")

        property_type = 'apartment'

        
        if rent>0:

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
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("furnished", furnished)
            #item_loader.add_value("floor", floor)
            #item_loader.add_value("parking", parking)
            #item_loader.add_value("elevator", elevator)
            #item_loader.add_value("balcony", balcony)
            
            #item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", 'immobiliare-eynard')
            item_loader.add_value("landlord_email", 'immobiliare@eynard-immobiliare.it')
            item_loader.add_value("landlord_phone", '035.428.48.57')
            item_loader.add_value("position", self.position)

            self.position+=1

            
            yield item_loader.load_item()