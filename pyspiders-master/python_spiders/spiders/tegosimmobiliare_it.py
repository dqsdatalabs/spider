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
    name = 'tegosimmobiliare_it'
    external_source = "Tegosimmobiliare_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=5&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=57&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq="
                ],      
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=55&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=36&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=56&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=63&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=7&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq=",
                    "http://tegosimmobiliare.it/elenco_immobili.asp?riferimento=&cod_istat=0&idtip=52&idcau=2&da_prezzo=&a_prezzo=&da_mq=&a_mq="
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

        for url in response.xpath("//ul[@class='properties-alternate']/li//div[@class='card-image']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")

        description=response.xpath("//h2[.='Descrizione']//following-sibling::p[2]/text()").get()
        if description:
            item_loader.add_value("description",description)
            address = description.split(",")[0]
            item_loader.add_value("address",address)
        info=response.xpath("//h2[.='Descrizione']//following-sibling::p[1]/text()").get()
        if info:
            rent=info.split("-")[-1].replace("€","")
            item_loader.add_value("rent",rent)
            external_id = info.split()[1]
            if external_id:
                if len(external_id) != 1:
                    item_loader.add_value("external_id",external_id)
                else:
                    external_id = f"{info.split()[1]}-{info.split()[3]}"
                    item_loader.add_value("external_id",external_id)
        square_meters=response.xpath("//span[.='Superficie:']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[-1].strip())
        room_count=response.xpath("//span[.='Camere:']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        room_count=response.xpath("//span[.='Camere:']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        balcony=response.xpath("//span[.='Balcone:']/following-sibling::span/text()").get()
        if balcony and "SI"==balcony:
            item_loader.add_value("balcony",True)
        terrace=response.xpath("//span[.='Terrazzo:']/following-sibling::span/text()").get()
        if terrace and "SI"==terrace:
            item_loader.add_value("terrace",True)
        elevator=response.xpath("//span[.='Ascensore:']/following-sibling::span/text()").get()
        if elevator and "SI"==elevator:
            item_loader.add_value("elevator",True) 
        images=response.xpath("//section[@class='properties main-content']//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.LatLng('")[-1].split(",")[0].replace("'",""))
        longitude=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.LatLng('")[-1].split(");")[0].split(",")[-1].replace("'",""))
 
        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","Florence Office")
        item_loader.add_value("landlord_phone","055485436")



        yield item_loader.load_item()