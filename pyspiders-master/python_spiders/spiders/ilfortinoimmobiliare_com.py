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

class IlfortinoimmobiliareSpider(scrapy.Spider):

    name = 'ilfortinoimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['ilfortinoimmobiliare.com']
    start_urls = ['https://www.ilfortinoimmobiliare.com/affitti.html?id_immobile=&cod_comune=&cod_tipologia=&cod_tipo_gestione=1&prezzo_da=0&prezzo_a=100000&superficie_da=0&superficie_a=1000&n_camere=&n_bagni=']

    position = 1
    def parse(self, response):
        pages = response.css('.pagine.no-cell a::attr(href)').getall()
        idx = (len(pages)-2)/2
        pages = pages[0:int(idx)]

        for page in pages:
            yield Request(
            url ="https://www.ilfortinoimmobiliare.com"+page,
             callback=self.parseApartment)


    def parseApartment(self, response):

        
        apartments = response.css('.thumbnail.bgblack')

        for i in range(0,len(apartments)):
            apartment = apartments[i].css('a::attr(href)').get()

            yield Request("https://www.ilfortinoimmobiliare.com"+apartment,callback=self.parseDetails)
        





    def parseDetails(self,response):

        external_id = response.url.split('/')[3].replace('scheda-',"")
        title = response.css('.col-md-12 h2::text').get()

        
        descriptionText = response.css('.mbottom50 *::text').getall()
        
        
        
        description = ''
        for text in descriptionText:
            description+=text.replace('\n,\t',"")
        description =  remove_white_spaces(description)
        room_count = 0
        bathroom_count=0
        square_meters=0
        rex =  re.findall(r'\d+ camere|\d+ singola|\d+ bagni|mq \d+',description)
        for i in rex:
            if 'camere' in i:
                room_count=int(i[0])
            if 'bagni' in i:
                bathroom_count=int(i[0])
            if 'singola' in i:
                room_count+=int(i[0])
            if 'mq' in i:
                square_meters+=int(i.split()[1])
        

       

        property_type=''
        address=''
        room_count = 0
        rows = response.xpath('//*[@class="table"]//tbody/tr')
        for row in rows:
            td1 = row.xpath('td//text()')[0].extract()
            try:
                td2 = row.xpath('td//text()').extract()[1]
                if 'Tipologia' in td1:
                    property_type = 'apartment' if 'Appartamento'  in td2 else 'house'
                if 'Vani' in td1:
                    room_count = int(td2)
                if 'Zona' in td1:
                    address = td2

            except:
                pass
            
        rent = 0
        price = response.css(".h4.colorff::text").get()
        price = price.replace('€ ',"").replace('.',"")
        if 'Trattative in agenzia' not in price and len(price)>0:
            rent = int(price)
        

        images = response.xpath('//*[@class="item"]//img/@src').getall()
        images = images[0:int(len(images)/2)]

        
        if rent>0:

            if room_count==0 or bathroom_count==0:
                RoomAndBaths_count = str(response.css('.main-features *::text'))
                rex =  re.findall(r'Bagni. \d+|Camere. \d+',RoomAndBaths_count)
                for i in rex:
                    if 'Camere' in i:
                        room_count=int(i[-1])
                    if 'Bagni' in i:
                        bathroom_count=int(i[-1])
                
            if bathroom_count==0:
                bathroom_count=1
            if room_count == 0:
                room_count=1

            
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", address)
            #item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            #item_loader.add_value("latitude", latitude)
            #item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            #item_loader.add_value("energy_label", energy_label)
            #item_loader.add_value("furnished", furnished)
            #item_loader.add_value("floor", floor)
            #item_loader.add_value("parking", parking)
            #item_loader.add_value("elevator", elevator)
            #item_loader.add_value("balcony", balcony)
            
            #item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", 'ilfortinoimmobiliare')
            item_loader.add_value("landlord_email", 'info@ilfortinoimmobiliare.com')
            item_loader.add_value("landlord_phone", '+39 339 880755')
            item_loader.add_value("position", self.position)

            self.position+=1
            yield item_loader.load_item()