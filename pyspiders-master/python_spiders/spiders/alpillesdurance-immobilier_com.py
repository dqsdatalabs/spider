# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from collections import deque
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'alpillesdurance-immobilier_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Alpillesduranceimmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "cod_tipologia" : 1,
                "property_type" : "apartment"
            },
            {
                "cod_tipologia" : 2,
                "property_type" : "house"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            cod_tipologia = str(url.get("cod_tipologia"))

            data = {
                "nature": "2",
                "type[]": cod_tipologia,
                "price":"" ,
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "currency": "EUR",
                "homepage": ""
            }

            yield FormRequest(
                "https://www.alpillesdurance-immobilier.com/en/search/",
                formdata=data,
                callback=self.parse,
                meta = {"property_type":url.get("property_type"),"cod_tipologia":cod_tipologia}
            )
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='ad']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        adres=response.xpath("//div[@class='path']/p/span/text()").get()
        if adres:
            item_loader.add_value("address",adres.strip().split(" ")[1])
        title=response.xpath("//div[@class='title']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h2[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        room_count=response.xpath("//span[contains(.,'rooms')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        square_meters=response.xpath("//span[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip().split(" ")[0])
        terrace=response.xpath("//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        images=[x for x in response.xpath("//div[@class='item resizePicture']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//p[@class='comment']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        energy_label=response.xpath("//h3[.='Energy efficiency']/following-sibling::img/@src").get()
        if energy_label:
            e_label=energy_label.split("/")[-1]
            e_label=energy_label_calculate(e_label)
            item_loader.add_value("energy_label",e_label)
        item_loader.add_value("landlord_name","ALPILLES DURANCE")
        item_loader.add_value("landlord_phone","+33 (0)4 90 92 87 17")

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label