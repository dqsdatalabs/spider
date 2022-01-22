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
import re
class MySpider(Spider):
    name = 'freedom-nieruchomosci_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="FreedomNieruchomosci_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.freedom-nieruchomosci.pl/nieruchomosci?type=OM&trans=W&ln=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.freedom-nieruchomosci.pl/nieruchomosci?type=OD&trans=W&ln=",
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
        border=response.xpath("//li[@class='all-pages']/text()").get()
        for item in response.xpath("//div[@class='row']//div/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )      
            seen = True
        if page == 2 or seen:
            if border and page<int(border)+1:   
                nextpage=response.xpath("//li[@class='next']/a/@href").get()
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
        rent=response.xpath("//td[.='Cena: ']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ",""))
        item_loader.add_value("currency","PLN")
        title=response.xpath("//div[@id='offer']/div/h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=" ".join(response.xpath("//div[@class='fbox']/table//td//text()").extract())
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//div[.='powierzchnia']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(",")[0])
        room_count=response.xpath("//div[.='liczba pokoi']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        deposit=response.xpath("//td[.='Kaucja: ']/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit)
        parking=response.xpath("//td[.='Garaż/Miejsca parkingowe: ']/following-sibling::td/text()").get()
        if parking and "Tak"==parking:
            item_loader.add_value("parking",True)
        description="".join(response.xpath("//div[@class='tab__content']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//picture//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Freedom Nieruchomosci")
        furnished=response.xpath("//td[.='Umeblowanie: ']/following-sibling::td/text()").get()
        if furnished and "Tak"==furnished:
            item_loader.add_value("furnished",True)
        balcony=response.xpath("//td[.='Liczba balkonów: ']/following-sibling::td/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)

        landlord_phone=response.xpath("//div[@class='hider']/@tel").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_name", "Freedom-Nieruchomosci")
        yield item_loader.load_item()