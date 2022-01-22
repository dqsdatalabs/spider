import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs

class GiorgioviganoSpider(scrapy.Spider):
        
    name = 'giorgiovigano'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['www.giorgiovigano.com']
    
    start_urls = ['https://www.giorgiovigano.com/en/property/?location=italia&prop_type%5B%5D=Apartment&prop_type%5B%5D=Penthouse&prop_type%5B%5D=Villa&size_min=&size_max=&price=&list_type%5B%5D=rent']

    position = 1 


    def parse(self, response):
        
        
        cards = response.css("#propertyList .property")
        

        for index, card in enumerate(cards):

            position = self.position
            
            card_url = card.css(".photo a::attr(href)").get()

            
            dataUsage = {
                "position": position,
                "card_url": card_url,
            }
            
            
            
            GiorgioviganoSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
        
        
        nextPageUrl = response.css("#pagi .page-numbers.next::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = nextPageUrl
            
        
        if nextPageUrl:
            yield Request(url = nextPageUrl, callback = self.parse, dont_filter=True)
      






    def parseApartment(self, response):

        property_type = response.css(".pinfo li:contains('Type') span::text").get()
        if property_type:
            property_type = property_type_lookup.get(property_type,"apartment")

        external_id = response.css(".pinfo .spaced .ref::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        
        square_meters = response.css(".pinfo li:contains('Size') span::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)
        
        room_count = response.css(".pinfo li:contains('Beds') span::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1 

        bathroom_count = response.css(".pinfo li:contains('Baths') span::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
        else:
            bathroom_count = 1             

        rent = response.css(".pinfo li:contains('Rent') span::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".","")


        currency = response.css(".pinfo li:contains('Rent') span::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"
        
        title = response.css(".mainimage h1::text").get()
        if title:
            title = remove_white_spaces(title)
        
        
        map = response.css('iframe::attr(src)').get()
        address = parse_qs(map)['q'][0]
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
        
        description = response.css('#home p::text').getall()[:-1]
        description = " ".join(description)
        description = remove_white_spaces(description)
  
            
        
        images = response.css('#photo_gallery_container #prop_photos img::attr(src)').getall()
        external_images_count = len(images)
        

        furnished = response.css("ul.features li:contains('Furniture')::text").get()
        if furnished:
            furnished = remove_white_spaces(furnished)
            if furnished == "Fully Furnished":
                furnished = True
            elif furnished == "Partly Furnished":
                furnished = True
            elif furnished == "Not Furnished":
                furnished = False
            else:
                furnished = False
        
        elevator = response.css("ul.features li:contains('Features')::text").get()
        if elevator:
            if "Lift" in elevator:
                elevator = True
            else:
                elevator = False

        balcony = response.css("ul.features li:contains('Features')::text").get()
        if balcony:
            if "Balcony" in balcony:
                balcony = True
            else:
                balcony = False
        
        
        terrace = response.css("ul.features li:contains('Features')::text").get()
        if terrace:
            if "Terrace" in terrace:
                terrace = True
            else:
                terrace = False
    
        
        landlord_name = "Giorgio Viganò Srl"
        
        landlord_email = response.css(".pinfo a.btn-dark::attr(href)").get()
        if landlord_email:
            landlord_email = remove_white_spaces(landlord_email).split("mailto:")[1]
        else:
            landlord_email = "info@giorgiovigano.com"
        
        landlord_phone = response.css(".pinfo p:contains('Call Us') a::attr(href)").get()
        if landlord_phone:
            landlord_phone = remove_white_spaces(landlord_phone).split("tel:")[1]
        else:
            landlord_phone = "+39027636151"


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
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
