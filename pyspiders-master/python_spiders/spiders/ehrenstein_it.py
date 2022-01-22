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

    name = 'ehrenstein'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    

    position = 1
    def start_requests(self):
        start_url = "https://ehrenstein.it/data.asmx/getSearchHome"

        payload = {"aData":["-1","","","1","1,12","","","","","","","","","","-1",0]}

        yield Request(start_url,headers={
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, br',
            'Content-Type':'application/json'
        },body=json.dumps(payload),
                method='POST'
                ,callback=self.parse)


    def parse(self, response):

        apartments = json.loads(response.text)['d']

        for apartment in apartments:

            external_id = str(apartment['IMB_PROTOKOLL'])
            square_meters = apartment['Mq']
            rent = re.search(r'\d+',apartment['Preis'].replace('.',''))[0]
            external_link = 'https://ehrenstein.it/detail_deu.aspx?id=' + apartment['IMB_IMB_ID']
            title = 'Property ref: '+external_id
            room_count = apartment['Zimmer']
            bathroom_count = apartment['Bad']
            property_type = 'apartment' if 'Wohnung' in apartment['Kategorie'] else 'house'

            dataUsage = {
                "property_type": property_type,
                "title":title,
                "external_id": external_id,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "rent": rent,
            }

            yield Request(external_link,meta=dataUsage,callback=self.parseApartment)


    def parseApartment(self,response):
        
        
        property_type     =response.meta["property_type"]    
        title             =response.meta['title']   
        external_id       =response.meta["external_id"]      
        square_meters     =response.meta["square_meters"]    
        room_count        =response.meta["room_count"]   
        bathroom_count    =response.meta["bathroom_count"]   
        rent              =response.meta["rent"]   

        city = response.css(".table.table-striped tr:contains('Gemeinde') td *::text").getall()[1]
        address =city +", "+ response.css(".table.table-striped tr:contains('Viertel') td *::text").getall()[1]

        
        furnished = True if response.css(".table.table-striped tr:contains('Komplett möbliert') td") else False
        parking = True if response.css(".table.table-striped tr:contains('Garage') td *:contains('Ja')") else False
        balcony = True if response.css(".table.table-striped tr:contains('Balkon') td *:contains('Ja')") else False
        terrace = True if response.css(".table.table-striped tr:contains('Terrasse') td *:contains('Ja')") else False
        elevator = True if response.css(".table.table-striped tr:contains('Aufzug') td *:contains('Ja')") else False

        floor = ''
        if len(response.css(".table.table-striped tr:contains('Stock') td *::text").getall())>1:
            try:
                floor = str(int(response.css(".table.table-striped tr:contains('Stock') td *::text").getall()[1][0]))
            except:
                floor = ''
        energy_label = ''
        if len(response.css(".table.table-striped tr:contains('Energieklasse') td *::text").getall())>1:
            try:
                energy_label = str(response.css(".table.table-striped tr:contains('Energieklasse') td *::text").getall()[1])
            except:
                energy_label = ''
        description= remove_white_spaces("".join(response.css('.text-md-left.offset-top-50 p *::text').getall()))
        images = response.css('a.thumbnail-classic::attr(href)').getall()
        images = ['https://ehrenstein.it/'+x for x in images] 

        if (int(rent)>0):

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)

            
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("city", city)
            item_loader.add_value("floor", floor)
            item_loader.add_value("address", address)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value('description',description)
            
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

            item_loader.add_value("parking", parking)

            item_loader.add_value("landlord_name", 'ehrenstein')
            item_loader.add_value("landlord_email", 'info@ehrenstein.it')
            item_loader.add_value("landlord_phone", '0471/983452')
            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
            




    