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
    name = 'realestate24_com_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Realestate24_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://realestate24.com.pl/pl/oferta?locale=M&transaction=W&miasto%5B%5D=&powuz%5B%5D=&powuz%5B%5D=&cena%5B%5D=&cena%5B%5D=&yt0=arama",
                "property_type" : "apartment"
            },
            {
                "url" : "https://realestate24.com.pl/pl/oferta?locale=D&transaction=W&miasto%5B%5D=&powuz%5B%5D=&powuz%5B%5D=&cena%5B%5D=&cena%5B%5D=&yt0=arama",
                "property_type" : "house"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        border=" ".join(response.xpath("//div[@class='pagination']/ul//li//a//text()").getall())
        border=border.split(" Następna")[0].split(" ")[-1]
        for item in response.xpath("//div[@id='photo-offers-list']/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        if page == 2 or seen:
            if border and page<int(border)+1 and not "current" in border:
                nextpage=f"https://realestate24.com.pl/pl/oferta/locale/M/transaction/W/miasto[]//powuz[]//cena[]//yt0/arama/id/52/p/{page}"
                if nextpage:      
                    yield Request(
                        response.urljoin(nextpage),
                        callback=self.parse,
                        dont_filter=True,
                        meta={"page":page+1,"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type=response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type",property_type)
        title=response.xpath("//h1[@class='h1']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//td[.='Lokalizacja']/following-sibling::td/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@id='rent-photo-price']/h4/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("PLN")[0].split(".")[0].replace(" ",""))
        item_loader.add_value("currency","PLN")
        deposit=response.xpath("//td[.='Kaucja']/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("zł")[0])
        room_count=response.xpath("//td[.='Liczba pokoi']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//td[.='Powierzchnia użytkowa']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        description="".join(response.xpath("//h2[.='Opis']/following-sibling::p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='col-md-2']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        balcony=response.xpath("//p[contains(.,'Balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        dishwasher=response.xpath("//p[contains(.,'Lodówka')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
        parking=response.xpath("//p[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        washing_machine=response.xpath("//p[contains(.,'Parking')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        name=response.xpath("//div[@style='padding: 20px;']/h3/text()").get()
        if name:
            item_loader.add_value("landlord_name",name.strip())
        email=response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1].strip())
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1].strip())
        yield item_loader.load_item()