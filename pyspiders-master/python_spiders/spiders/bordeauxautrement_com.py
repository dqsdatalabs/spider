# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import S
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math
class MySpider(Spider):
    name = 'bordeauxautrement_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Bordeauxautrement_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED":False,
    }
    headers={
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": "_ga=GA1.2.603844230.1638189866; _fbp=fb.1.1638189867265.812814450; cookies-accepted=1; PHPSESSID=2lbgnuub9mdglrrf8lesfi9k79; _gid=GA1.2.1068742483.1638430438; _gat_UA-45247292-1=1; _uetsid=35da3540534211ec974fdf2761ecbcde; _uetvid=161ed660511211ecb19fdb962a393e4d",
        "referer": "https://www.bordeauxautrement.com/fr/search/long?_filter%5Bprices%5D=1000%2C3000&_filter%5Bchambers%5D=0%2C4&_filter%5Bsort%5D=&outOfCity=&search=1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.bordeauxautrement.com/fr/search/ajax/long?arrivalDate=&departureDate=&chamber=&type=&outOfCity=&_filter%5Bsort%5D=&_filter%5Bprices%5D=1000%2C3000&_filter%5Bchambers%5D=0%2C4&search=1",
            },           
        ] #LEVEL-1
        for url in start_urls:
            yield Request(url=url.get('url'),headers=self.headers,
                                 callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)["list"]
        for item in data:
            f_url = item['url']
            yield Request(
                response.urljoin(f_url), 
                callback=self.populate_item,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//td[.='Loyer mensuel']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("\xa0","").split("€")[0])
        item_loader.add_value("currency","EUR")
        available_date=response.xpath("//td[.='Disponible']/following-sibling::td/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        square_meters=response.xpath("//ul[@class='details pvl']//li//span//text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        description="".join(response.xpath("//div[@class='appartment-section-heading']/following-sibling::div//p//strong//text()").getall())
        if description:
            item_loader.add_value("description",description)
        property_type="".join(response.xpath("//div[@class='appartment-section-heading']/following-sibling::div//p//strong//text()").getall())
        if property_type:
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
        latitude=response.xpath("//div[@class='map-single']/@data-latlng").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split(",")[0].replace("[",""))
        longitude=response.xpath("//div[@class='map-single']/@data-latlng").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split(",")[1].replace("]",""))
        images=[response.urljoin((x.split("background-image: url('")[1].split("');")[0])) for x in response.xpath("//li[contains(@style,'background-image:')]/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        parking=response.xpath("//span[.='Parking']").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator=response.xpath("//span[.='Ascenseur']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        balcony=response.xpath("//span[.='Balcon']").get()
        if balcony:
            item_loader.add_value("balcony",True)
        adres=response.xpath("//span[@class='appartment-name']/text()").get()
        if adres:
            item_loader.add_value("address",adres)

        yield item_loader.load_item()