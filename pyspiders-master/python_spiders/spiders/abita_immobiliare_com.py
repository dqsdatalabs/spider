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


class Abita_immobiliare_Spider(scrapy.Spider):

    name = 'abita_immobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['abita-immobiliare.com']
    start_urls = ['https://www.abita-immobiliare.com/r-immobili/?Codice=&Motivazione%5B%5D=2&Tipologia%5B%5D=1&Tipologia%5B%5D=31&Tipologia%5B%5D=2&Tipologia%5B%5D=9&Tipologia%5B%5D=10&Prezzo_a_da=&Prezzo_a_a=&Locali_da=&Camere_da=&Bagni_da=&Totale_mq_da=&Totale_mq_a=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0']

    position = 1

    def parse(self, response):
        '.realestate-griglia .titolo'
        apartments = response.css('.realestate-griglia')

        
        for apartment in apartments:
            url = apartment.css('figure a::attr(href)').get()
            title = remove_white_spaces(apartment.css('.titolo::text').get())
            rent = int(extract_number_only(apartment.css('.prezzo::text').get()))
            external_id = apartment.css('.codice::text').get().replace("Cod. ","")
            yield Request(url,meta={
                'external_id':external_id,
                'title':title,
                'rent':rent
                },
                callback=self.parseDetails)

    def parseDetails(self, response):

        external_link = response.url
        external_id = response.meta['external_id']
        title = response.meta['title']

        zipcode = ''
        city = ''
        address =''
        try:
            latitude = float(response.css("meta[itemprop='latitude']::attr(content)").get())
            longitude = float(response.css("meta[itemprop='longitude']::attr(content)").get())

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)

        except:
            longitude = ""
            latitude = ""

        if address == "":
            address = title
       

        room_count = response.css('.boxCx .ico span::text').getall()[1]
        try:
            bathroom_count = response.css('.boxCx .ico span::text').getall()[2]
        except:
            bathroom_count = 1
        try:
            rex = re.search(
            r'\d+', response.css('.boxCx .ico span::text').getall()[0])
            if rex:
                square_meters = rex[0]
            else:
                square_meters = 0
        except:
            square_meters = 0
        
        info = response.css("#sezInformazioni .box *::text").getall()
        parking = False
        floor=""
        furnished = False
        for i in info:
            if 'Piano' in i:
                floor = i.split(':')[1]
            if 'Arredato: Arredato' in i:
                furnished = True
            if 'Posto auto' in i:
                parking = True
            
        energy_label = response.css(".classe_energetica span *::text").get()

        rent = response.meta['rent']
        description = "".join(response.css('.dettaglio p::text').getall())
        description = remove_white_spaces(description)
        if 'Villa' in description:
            property_type = 'house'
        else:
            property_type = 'apartment'

        images = response.css('.ClickZoomGallery img::attr(data-src)').getall()
        images = [x.replace('/thumb','') for x in images]

       

        if rent > 0:

            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value('external_id',external_id)
            item_loader.add_value("title", title)
            item_loader.add_value('energy_label',energy_label)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value('furnished',furnished)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value('floor',floor)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value(
                "square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

            item_loader.add_value("parking", parking)
            item_loader.add_value("landlord_name", 'Abita - Real Estate Agency ')
            item_loader.add_value(
                "landlord_email", 'info@abita-immobiliare.com')
            item_loader.add_value("landlord_phone", '334 2752347')

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()


        
