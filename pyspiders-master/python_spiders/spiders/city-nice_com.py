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
    name = 'city-nice_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Citynice_PySpider_france"
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
                "price":"", 
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }

            yield FormRequest(
                "http://www.city-nice.com/fr/recherche/",
                formdata=data,
                callback=self.parse,
                meta = {"property_type":url.get("property_type"),"cod_tipologia":cod_tipologia}
            )
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='ads']//a[@class='button']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title: 
            item_loader.add_value("title",title)
        item_loader.add_value("address","Nice")
        description="".join(response.xpath("//p[@class='comment']/text()").get())
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//li[contains(.,'Mois')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//span[contains(.,'pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        square_meters=response.xpath("//li[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        images=[x for x in response.xpath("//div[@class='item resizePicture']/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        utilities=response.xpath("//li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0])
        deposit=response.xpath("//li[contains(.,'Dépôt de garanti')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])

        yield item_loader.load_item()