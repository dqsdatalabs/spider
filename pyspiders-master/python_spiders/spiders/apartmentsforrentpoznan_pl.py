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
import math
class MySpider(Spider):
    name = 'apartmentsforrentpoznan_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="ApartmentsForRentPoznan_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.apartments-for-rent-poznan.pl/",
                "room_count":"1"
            },  
            {
                "url" : "https://www.apartments-for-rent-poznan.pl/2-bedrooms",
                "room_count":"2"
            },     
            {
                "url" : "https://www.apartments-for-rent-poznan.pl/3-bedrooms",
                "room_count":"3"
            },     
            {
                "url" : "https://www.apartments-for-rent-poznan.pl/houses",
                
            },     
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,meta={'room_count': url.get('room_count')})
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//section//a[@data-testid='linkElement']/@href").getall():
            if not "mailto:info@apartments-for-rent-poznan.pl" in item:
                f_url = response.urljoin(item)
                yield Request(
                    f_url, 
                    callback=self.populate_item,meta={'room_count': response.meta.get('room_count')} 
                )

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        room_count=response.meta.get("room_count")
        if room_count:
            item_loader.add_value("room_count",room_count)
        rent=response.xpath("//h2[@class='font_2']//span[.='Monthly:']/parent::span/following-sibling::span[2]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("'",""))
        item_loader.add_value("currency","PLN")
        title=response.xpath("//span[@style='font-family:futura-lt-w01-book,sans-serif']/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//span[@style='font-family:futura-lt-w01-book,sans-serif']/text()").get()
        if property_type:
            if "apartment" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "house" in property_type.lower():
                item_loader.add_value("property_type","house")
        adres="".join(response.xpath("//h3[@class='font_2']//text()").getall())
        if adres:
            item_loader.add_value("address",adres.replace("|","").replace(" ","").replace("Str.",""))
        bathroom_count="".join(response.xpath("//h2//span[@style='font-family:futura-lt-w01-book,sans-serif']/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("bathroom")[0].split("|")[-1].replace("\xa0","").strip())
        square_meters="".join(response.xpath("//h2//span[@style='font-family:futura-lt-w01-book,sans-serif']/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("Total area:")[-1].split("m2")[0])
        description="".join(response.xpath("//span[@style='font-size:14px']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        utilities=response.xpath("//span[.='* Bills']/parent::p/parent::div/following-sibling::div/p//span[contains(.,'PLN')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("PLN")[-1].strip())
        deposit=response.xpath("//span[.='** Deposit: ']/parent::p/parent::div/following-sibling::div/p//span[contains(.,'PLN')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("PLN")[-1].strip())
        images=[x.split(" ")[0] for x in response.xpath("//picture//source/@srcset").getall()]
        if images:
            item_loader.add_value("images",images)
        furnished=response.xpath("//span[contains(.,'furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        dishwasher=response.xpath("//span[contains(.,'Washer')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
        pets_allowed=response.xpath("//span[contains(.,'Pets friendly')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed",True)
        item_loader.add_value("landlord_name","Apartments for rent Poznan")
        yield item_loader.load_item()