# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider): 
    name = 'ilperuzzicasa_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Ilperuzzicasa_PySpider_italy"
    url = "https://www.ilperuzzicasa.it/it/ricerca.asp?tp=immobili-affitto"
    
    def start_requests(self):
    
        yield Request(
            url=self.url,
            callback=self.parse,
        )
    def parse(self, response):
        for item in response.xpath("//div[@class='tab-ric']//a//@href").getall():
            property_type=response.xpath("//h2[@class='span2-ric']/text()").get()
            yield Request(response.urljoin(item), callback=self.populate_item,meta={"property_type":property_type})
        next_page = response.xpath("//a[@id='pagavanti']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//td/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        desc=" ".join(response.xpath("//div[@class='tornaric']/following-sibling::p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        dontallow=response.xpath("//h1[@class='h1-det']/text()").get()
        if dontallow and "commerciale" in dontallow.lower():
            return 

        images=response.xpath("//div[@class='item']/a/@href").getall()
        if images:
            item_loader.add_value("images",images)
        if response.meta.get("property_type")=="APPARTAMENTO":
            item_loader.add_value("property_type","apartment")
        else:
            return 
        rent=response.xpath("//li[@class='price_ric']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(":")[-1].replace(".","").replace("€","").strip())
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//strong[.='Rif.:']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        square_meters=response.xpath("//strong[.='Superficie Mq:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        room=response.xpath("//strong[.='Numero vani:']/following-sibling::text()").get()
        if room:
            item_loader.add_value("room_count",room)
        floor=response.xpath("//strong[.='Piano:']/following-sibling::text()").get()
        if floor and not "-" in floor:
            item_loader.add_value("floor",floor.replace("&nbsp;",""))
        energy_label=response.xpath("//strong[.='Classe Energetica:']/following-sibling::text()").get()
        if energy_label and not "-" in energy_label:
            item_loader.add_value("energy_label",energy_label.replace("&nbsp;",""))
        elevator=response.xpath("//strong[.='Ascensore:']/following-sibling::text()").get()
        if elevator and "si" in elevator:
            item_loader.add_value("elevator",True)
        item_loader.add_value("landlord_name","IL PERUZZI IMMOBILIARE")

        item_loader.add_value("landlord_phone","055 6560386")
        item_loader.add_value("landlord_email","info@ilperuzzicasa.it")

        position = response.xpath("//iframe[@id='iframe_geomap_canvas']/@src").get()
        if position:
            lat = position.split("Latitudine:")[-1].split()[0]
            long = position.split("Longitudine:")[-1].strip(").")
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        address = response.xpath("//td/h2/text()").get()
        if address:
            city = address.split("-")[-1]
            item_loader.add_value("city",city)
            item_loader.add_value("address",address)


        yield item_loader.load_item() 