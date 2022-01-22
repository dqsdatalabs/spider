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
    name = 'wrbconseil-immobilier_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="Wrbconseilimmobilier_PySpider_france"
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.wrbconseil-immobilier.com/en/listing-location.html",
                ],
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='item-link']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//div[@class='spin4 info_prix']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace("\xa0","").strip())
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//li[contains(.,'rooms')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//li/sup[.='2']/parent::li/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].strip())
        adres=response.xpath("//i[@class='tea ion-ios-location-outline']/following-sibling::text()").get()
        if adres:
            item_loader.add_value("address",adres)
        images=[x for x in response.xpath("//img[@class='img-fluid']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        external_id=response.xpath("//li[@class='c_numero']//span[@class='champ']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        bathroom_count=response.xpath("//li[@class='c_sbain']//span[@class='champ']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        terrace=response.xpath("//li[@class='c_nbterrasse']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking=response.xpath("//li[@class='c_parking']").get()
        if parking:
            item_loader.add_value("parking",True)
        latitude=response.xpath("//script[contains(.,'centerLngLat ')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("centerLngLat")[-1].split("};")[0].split(",")[0].split(":")[-1].replace("'","").replace('"',""))
        longitude=response.xpath("//script[contains(.,'centerLngLat ')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("centerLngLat")[-1].split("};")[0].split(",")[-1].split(":")[-1].replace("'","").replace('"',""))

        yield item_loader.load_item()