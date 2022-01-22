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
    name = 'kghurst_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Kghurst_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.kghurst.com.au/listings/?saleOrRental=Rental&doing_wp_cron=1641892470.0232288837432861328125"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        data=str(response.body).split("var MapDataStore =")[-1].split("collapseHeaderWithMap")[0].strip().replace(";\n\t\tvar","")
        for item in data.split('"url":"'):
            follow_url = response.urljoin(item.split(",")[0].replace("\\","").replace('"',""))
            yield Request(follow_url, callback=self.populate_item)  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//p[@class='single-listing-address ']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//p[@class='listing-info-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("per")[0].strip())
        item_loader.add_value("currency","EUR")
        description=response.xpath("//div[@class='section-body post-content']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        latitude=response.xpath("//script[contains(.,'postLat')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("postLat")[-1].split(";")[0].replace("=","").strip())
        longitude=response.xpath("//script[contains(.,'postLong')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("postLong")[-1].replace("=","").split(";")[0].strip())
        adres=response.xpath("//h5[@class='staff-card-title']/a/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        deposit=response.xpath("//strong[.='bond price']/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("$")[-1].strip())
        room_count=response.xpath("//p[@class='listing-attr icon-bed']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//p[@class='listing-attr icon-bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=[x.split("url('")[-1].split("')")[0] for x in response.xpath("//div[@class='gallery-image-fill']/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        external_id=response.xpath("//strong[.='property ID']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        parking=response.xpath("//p[@class='listing-attr icon-car']/text()").get()
        if parking and parking=="1": 
            item_loader.add_value("parking",True)
        name=response.xpath("//h5[@class='staff-card-title']/a/@href").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[@class='phone-number__show brand-fg']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        property_type=response.url 
        if property_type and "residential" in property_type:
            item_loader.add_value("property_type","apartment")
        
        yield item_loader.load_item()