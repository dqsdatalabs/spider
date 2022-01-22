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
import dateparser
class MySpider(Spider):
    name = 'tridentholidayhomes_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source="Tridentholidayhomes_PySpider_france"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.tridentholidayhomes.ie/long-term-rental/holidays-rentals-rentals-r0/",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[.='+ INFO']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        property_type=response.xpath("//h1[@class='detail-bien-type']/span/text()").get()
        if property_type:
            if "House" in property_type:
                item_loader.add_value("property_type","house")
        square_meters=response.xpath("//i[@class='icon icon-metros']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//i[@class='icon icon-camas']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//i[@class='icon icon-bath']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        rent=response.xpath("//span[@id='precio']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].strip().replace(",",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//li[@class='address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//li[@class='address']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-2])
        description=response.xpath("//div[@id='descriptionText']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@id='galleryGrid']//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        parking=response.xpath("//span[.='Parking']").get()
        if parking:
            item_loader.add_value("parking",True)
        garden=response.xpath("//span[.='Garden']").get()
        if garden:
            item_loader.add_value("terrace",True)
        item_loader.add_value("landlord_name","Trident Holiday Homes")
        
       
        
        yield item_loader.load_item()
        

