# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'agofim_com'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Agofim_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agofim.com/r/annunci/affitto-appartamento-.html?Codice=&Tipologia%5B%5D=1&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes",
                    "https://www.agofim.com/r/annunci/affitto-attico-.html?Codice=&Tipologia%5B%5D=37&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes"
                ],      
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.agofim.com/r/annunci/affitto-casa-indipendente-.html?Codice=&Tipologia%5B%5D=36&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes",
                    "https://www.agofim.com/r/annunci/affitto-casale-.html?Codice=&Tipologia%5B%5D=140&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes",
                    "https://www.agofim.com/r/annunci/affitto-villa-.html?Codice=&Tipologia%5B%5D=9&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes",
                    "https://www.agofim.com/r/annunci/affitto-villetta-.html?Codice=&Tipologia%5B%5D=49&Motivazione%5B%5D=2&Provincia=0&Comune=0&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&cf=yes"
                ],
                "property_type" : "house" 
            }
          
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//ul/li/section//a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        pagination = response.xpath("//div[@class='paging']//a[@class='next']/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")
        city = response.xpath("//strong[contains(.,'Provincia')]/parent::div/text()").get()
        if city:
            item_loader.add_value("city", city.split(':')[-1].strip())
            
        address = response.xpath("//h2/text()[contains(.,'Zona')]").get()
        if address:
            if address:
                item_loader.add_value("address", city.split(':')[-1].strip() + " " + address.split(':')[-1].strip())
        else:
            address = response.xpath("//strong[contains(.,'Provincia')]/parent::div/text()").get()
            if address:
                item_loader.add_value("address", address.split(':')[-1].strip())
    
        item_loader.add_xpath("external_id", "substring-after(//div[strong[.='Codice']]/text(),': ')")
        item_loader.add_xpath("rent_string", "substring-after(//div[strong[.='Prezzo']]/text()[.!='Tratt. riservata'],': ')")
        
        item_loader.add_xpath("square_meters", "substring-before(substring-after(//div[strong[.='Totale mq']]/text(),': '),'m')")
        room_count = response.xpath("//span[@class='ico-24-locali']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('Locali')[0].strip())
        item_loader.add_xpath("bathroom_count", "substring-after(//div[strong[.='Bagni']]/text(),': ')")
        item_loader.add_xpath("floor", "substring-after(//div[strong[.='Piano']]/text(),': ')")
        item_loader.add_xpath("energy_label", "//div[@class='classe_energ']/div/text()")
        item_loader.add_xpath("utilities", "substring-after(//div[strong[.='Spese condominio']]/text()[.!=': € '],': ')")
     
        description = " ".join(response.xpath("//div[@class='testo']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        elevator = response.xpath("substring-after(//div[strong[.='Ascensore']]/text(),': ')").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[@id='det_terrazza']/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("substring-after(//div[strong[.='Balconi']]/text(),': ')").get()
        if balcony:
            item_loader.add_value("balcony", True)

        latlng = "".join(response.xpath("//script/text()[contains(.,'var lat')]").extract())
        if latlng:
            latitude = latlng.split('lat = "')[-1].split('"')[0].strip()
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('lgt = "')[-1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='swiper-wrapper']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Agofim Immobili & Finanze")
        item_loader.add_value("landlord_phone", "0116997346")
        item_loader.add_value("landlord_email", "info@agofim.com")

        yield item_loader.load_item()