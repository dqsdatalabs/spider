# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'imi_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Imi_PySpider_italy"
    url = "https://imi.it/ricerca-immobili/?contratto=affitto&zona=-1&prezzo_acquisto_max=-1&prezzo_affitto_max=-1"
    
    def start_requests(self):
    
        yield Request(
            url=self.url,
            callback=self.parse,
        )
    def parse(self, response):

        for item in response.xpath("//article[@class='item']/a/@href").getall():
            yield Request(item, callback=self.populate_item)

        
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        external_id=response.xpath("//h3[@class='section-title'][contains(.,'RIF')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='page-title-xl']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//h3[@class='section-title'][contains(.,'mese')]/text()").get()
        if rent:
            rent=rent.split("/")[0].replace(".","").replace("€","").strip()
            item_loader.add_value("rent",rent)
        desc=" ".join(response.xpath("//div[@class='entry-content bodycopy']/p//text()").getall()) 
        if desc:
            item_loader.add_value("description",desc)
        images=response.xpath("//img//@src").getall()
        if images:
            for i in images:
                if "uploads" in i:
                    item_loader.add_value("images",i)
        square_meters=response.xpath("//p[.='Metri quadrati']/parent::div/following-sibling::div/p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        floor=response.xpath("//p[.='Piano']/parent::div/following-sibling::div/p/text()").get()
        if floor:
            item_loader.add_value("floor",floor.replace("\u00b0",""))
        bathroom_count=response.xpath("//p[.='Bagni']/parent::div/following-sibling::div/p/text()").get()
        if bathroom_count:
            if bathroom_count == "DUE":
                item_loader.add_value("bathroom_count",2)
            elif bathroom_count == "MONO":
                item_loader.add_value("bathroom_count",1)
            else:
                item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        else:
            item_loader.add_value("bathroom_count",1)
        elevator=response.xpath("//p[.='Ascensore']/parent::div/following-sibling::div/p/text()").get()
        if elevator and "SI"==elevator:
            item_loader.add_value("elevator",True)
        terrace=response.xpath("//p[.='Terrazzo']/parent::div/following-sibling::div/p/text()").get()
        if terrace and "SI"==terrace:
            item_loader.add_value("terrace",True)
        floor_plan_images=response.xpath("//h2[.='Planimetria']/following-sibling::a//img[@class='image']//@src").getall()
        if floor_plan_images and not floor_plan_images==[""]:
            item_loader.add_value("floor_plan_images",floor_plan_images)
        energy_label=response.xpath("//p[.='Classe energetica']/parent::div/following-sibling::div/p/text()").get()
        if energy_label:
            energy=energy_label.split("IPE")[0]
            if energy and "CLASSE" in energy:
                energy1=energy.split("CLASSE")[-1].split(" ")[1]
                if energy1:
                    item_loader.add_value("energy_label",energy1.replace(";",""))
        item_loader.add_value("landlord_name","IMI IMMOBILIARE")
        phone=response.xpath("//p[contains(.,'Tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split("Tel")[-1])
        email=response.xpath("//p[contains(.,'Tel')]/following-sibling::p/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)


        info = str(response.xpath("//p").getall())
        if "ARREDATO" in info:
            item_loader.add_value("furnished",True) 

        utilities=response.xpath("//p[.='Spese condominiali mese']/parent::div/following-sibling::div/p/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)

        
        item_loader.add_value("property_type","apartment")

        item_loader.add_value("currency","EUR")

        lat = response.xpath("//div[@class='marker']/@data-lat").get()
        long = response.xpath("//div[@class='marker']/@data-lng").get()
        if lat:
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        parking=response.xpath("//p[.='Posto auto']/parent::div/following-sibling::div/p/text()").get()
        if parking:
            if not "NO" in parking:
                item_loader.add_value("parking",True)

        item_loader.add_value("city","Milan")

        room_count = response.xpath("//p[.='Locali']/parent::div/following-sibling::div/p/text()").get()
        if "UFFICIO" in room_count.strip():
            return
        if room_count:
            item_loader.add_value("room_count",decide_room_count(room_count))        

        if "uffico" in desc.lower():
            return

        yield item_loader.load_item()


def decide_room_count(room_string):
    room_string:str
    if not room_string.isdigit():
        if "DUE" in room_string:
            return 2
        elif "OPEN" in room_string:
            return 1
        elif "TRE" in room_string:
            return 3
        else:
            return room_string
    else:
        return room_string