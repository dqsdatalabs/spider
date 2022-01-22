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
    name = 'encasa_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Encasa_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://encasa.pl/oferty/lista/?offer_type=1&order_type=1&room_count__gte=&room_count__lte=&search-partner-id=",
                "property_type" : "house"
            },
            {
                "url" : "https://encasa.pl/oferty/lista/?offer_type=0&order_type=1&room_count__gte=&room_count__lte=&search-partner-id=",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='property-image-main-photo']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
          
        nextpage=response.xpath("//li[@class='next']/a/@href").get()  
        if nextpage:      
            yield Request(
                response.urljoin(nextpage),
                callback=self.parse,
                dont_filter=True,
                meta={"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        rent=response.xpath("//div[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("\xa0","").replace(" ","").replace("\n","").strip())
        item_loader.add_value("currency","PLN")

        square_meters=response.xpath("//label[.='powierzchnia']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].split("m")[0].replace("\xa0",""))
        title=response.xpath("//div[@class='describe col-md-9 col-sm-9']//h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        room_count=response.xpath("//label[.='liczba pokoi']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        description="".join(response.xpath("//div[@class='description-text']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[response.urljoin(x) for x in response.xpath("//figure//img/@data-image").getall()]
        if images:
            item_loader.add_value("images",images)
        adres=response.xpath("//div[@class='street']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        name=response.xpath("//div[@class='contact-box']//div[@class='name']/text()").get()
        item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[@class='show-number no-desktop']/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[1].split("+")[1])
        yield item_loader.load_item()