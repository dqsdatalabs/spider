# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 
import dateparser
import re

class MySpider(Spider):
    name = 'immodicasa_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Immodicasa_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://dicasa.be/te-huur"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//div[@class='contentdiv']/following-sibling::a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
            seen=True
        if page == 1 or seen:
            nextpage=f"https://dicasa.be/te-huur?page={page}#contenttop"
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse,meta={'property_type': response.meta.get('property_type'),'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        dontallow=response.xpath("//span[@class='price sold']/text()").get()
        if dontallow and "VERHUURD"==dontallow:
            return 
        title=response.xpath("//div[@class='col-lg-5 sameheight']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=title 
        if property_type and "appartement" in title.lower():
            item_loader.add_value("property_type","apartment")
        if property_type and ("woning" in title or "woonst" in title):
            item_loader.add_value("property_type","house")
        if property_type and "flat" in title:
            item_loader.add_value("property_type","apartment")
        if property_type and "studio" in title:
            item_loader.add_value("property_type","studio")
        if property_type and ("garage" in property_type.lower() or "stockage" in property_type.lower() or "handel" in property_type.lower()):
            return 
        adres=response.xpath("//div[@class='col-lg-5 sameheight']/h2/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//td[.='Bewoonbare oppervlakte: ']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip())
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@class='col-lg-6 sameheight']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//td[.='Slaapkamers']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        elevator=response.xpath("//td[.='Lift aanwezig?']/following-sibling::td/text()").get()
        if elevator and elevator=="Ja":
            item_loader.add_value("elevator",True)
        latitude=response.xpath("//script[contains(.,'google.maps.Map(')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("google.maps.Map(")[0].split("uluru")[-1].split("};")[0].split(",")[0].replace("{","").split("lat:")[-1].strip())
        longitude=response.xpath("//script[contains(.,'google.maps.Map(')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("google.maps.Map(")[0].split("uluru")[-1].split("};")[0].split(",")[-1].replace("{","").split("lng:")[-1].strip())
        item_loader.add_value("landlord_name","Dicasa")
        item_loader.add_value("landlord_phone","051 20 14 00")
        item_loader.add_value("landlord_email","info@dicasa.be")
        yield item_loader.load_item() 