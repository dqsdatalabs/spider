# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import re
import dateparser

class MySpider(Spider):
    name = 'casavelocefirenze_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Casavelocefirenze_PySpider_italy" 
    def start_requests(self):

        start_urls = [
            {
                "type" : 5,
                "property_type" : "apartment"
            },
            {
                "type" : 57,
                "property_type" : "apartment"
            },
            {
                "type" : 14,
                "property_type" : "apartment"
            },
            {
                "type" : 70,
                "property_type" : "studio"
            },
            {
                "type" : 34,
                "property_type" : "house"
            },
            {
                "type" : 58,
                "property_type" : "house"
            },
            {
                "type" : 52,
                "property_type" : "house"
            },
            {
                "type" : 7,
                "property_type" : "house"
            },
            {
                "type" : 63,
                "property_type" : "house"
            },
            {
                "type" : 51,
                "property_type" : "room"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            payload = {
                "riferimento": "",
                "idpr": "0",
                "cod_istat": "0",
                "idtip": r_type,
                "prezzo": "0",
                "nvani": "0",
                "idcau": "2",
                "ordinaper": "P",
                "Cerca": "Cerca",
            }
          
            
            yield FormRequest(url="http://www.casavelocefirenze.it/elenco_immobili_f.asp",
                            callback=self.parse,
                            formdata=payload,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//table[@class='elenco-standard']//h4/a/@href").getall():
            url = response.urljoin(item)
            yield Request(url, callback=self.populate_item,meta={"property_type" : response.meta.get("property_type")})
                
        pagination = response.xpath("//div[@id='paginazione']//a[b[.='Succ.']]/@href").get()
        if pagination:
            follow_url = response.urljoin(pagination)
            yield Request(follow_url, callback=self.parse,meta={"property_type" : response.meta.get("property_type")})

        
        
    # 2. SCRAPING level 2
    def populate_item(self, response,**kwargs):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title//text()")
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("_")[-1].split(".")[0])
        
        rent=response.xpath("//meta[@property='og:title']/@content").get()
        if rent:
            rent=rent.split("-")[-1].split("e")[0]
            if rent:
                item_loader.add_value("rent",rent)
        images=response.xpath("//a[@href='#fotoalta']/img/@src").getall()
        if images:
            item_loader.add_value("images",images)
        desc=" ".join(response.xpath("//b[.='Descrizione']/parent::font/following-sibling::b/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        square_meters=response.xpath("//font[.='Superficie']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[-1].replace("\xa0","").strip())
        energy_label=response.xpath("//font[.='Classe energetica']/following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        floor=response.xpath("//font[.='Piano']/following-sibling::text()").get()
        if floor:
            floor=floor.strip().split(" ")[-1]
            if floor.isdigit():
                item_loader.add_value("floor",floor)
        balcony=response.xpath("//font[.='Balcone']/following-sibling::text()").get()
        if balcony:
            if "NO" in balcony:
                item_loader.add_value("balcony",False)
            if "SI" in balcony:
                item_loader.add_value("balcony",True)
        elevator=response.xpath("//font[.='Ascensore']/following-sibling::text()").get()
        if elevator:
            if "NO" in elevator:
                item_loader.add_value("elevator",False)
            if "SI" in elevator:
                item_loader.add_value("elevator",True)
        terrace=response.xpath("//font[.='Terrazzo']/following-sibling::text()").get()
        if terrace:
            if "NO" in terrace:
                item_loader.add_value("terrace",False)
            if "SI" in terrace:
                item_loader.add_value("terrace",True)
        room_count=response.xpath("//font[.='Num. vani']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1])
        # bathroom_count=response.xpath("//font[.='Num. vani']/following-sibling::text()").get()
        # if bathroom_count:
        #     item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1])
        item_loader.add_value("landlord_phone","055-368505")
        item_loader.add_value("landlord_email","rentals@casavelocefirenze.it")

        address = response.xpath("//p/text()/following-sibling::text()").get()
        if address:
            address = address.split(":")[-1]
            item_loader.add_value("address",address)

        item_loader.add_value("landlord_name","FLORENCE VIA PONTE ALLE MOSSE 115")
        item_loader.add_value("currency","EUR")
        if address:
            city = address.split()[-1]
            if city == "(FI)":
                item_loader.add_value("city","Florence")
            elif city == "(AR)":
                item_loader.add_value("city","Arezzo")

        furnished = response.xpath("//b[contains(text(),'ARREDATO')]").get()
        if furnished:
            item_loader.add_value("furnished",True)

        
        yield item_loader.load_item()
