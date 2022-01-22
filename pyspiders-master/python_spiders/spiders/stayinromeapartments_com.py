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
    name = 'stayinromeapartments_com'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Stayinromeapartments_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.stayinromeapartments.com/noleggio-lungo-termine/affitti-appartamenti-c1/list-view/",
                ],      
                "property_type" : "apartment"
            }
           
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),"base_url":item})

    # 1. FOLLOWING
    def parse(self, response):
        base_url = response.meta.get('base_url')
        for item in response.xpath("//div[@class='line']/li//label/a"):
            url = item.xpath("./@href").get()
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        pagination = response.xpath("//div[@class='formato_pag']/a/@href").get()
        if pagination:
            follow_url = base_url+pagination
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"],"base_url":base_url})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        external_id = response.url
        if external_id:
            external_id = external_id.split('-')[-1].split('.html')[0]
            item_loader.add_value("external_id", external_id)
        
        rent=response.xpath("//span[@id='precio']/text()").get()
        if rent:
            rent=rent.split("€")[-1].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        item_loader.add_value("city","Rome")

        address = response.xpath("//span[@class='nombre']/a/text()").get()
        if address:
            item_loader.add_value("address", address.split('Via')[-1].strip())

        deposit=response.xpath("//span[contains(.,'Amount')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].replace("€","").replace(".",""))
        description=" ".join(response.xpath("//div[@id='descriptionText']/text()").getall())
        if description:
            item_loader.add_value("description",description)

        bathroom_count=response.xpath("//div[@class='bathroom-item ']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        square_meters=response.xpath("//i[@class='icon icon-metros']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        room_count1=response.xpath("//i[@class='icon icon-camas']/following-sibling::span/text()").get()
        room_count2=response.xpath("//i[@class='icon icon-door']/following-sibling::span/text()").get()
        if room_count1 and room_count2:
            item_loader.add_value("room_count",int(room_count1)+int(room_count2))
        latitude=response.xpath("//input[@name='latitud']/@value").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//input[@name='longitud']/@value").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        images=[x for x in response.xpath("//div[@class='owl-carousel photo-gallery']//a//@href").getall()]
        if images:
            item_loader.add_value("images",images)
        landlord_name="StayInRome Apartments"
        item_loader.add_value("landlord_name",landlord_name)
        landlord_phone="+39 06 90203088"
        item_loader.add_value("landlord_phone",landlord_phone)
        landlord_email="info@stayinromeapartments.com"
        item_loader.add_value("landlord_email",landlord_email)

     

        yield item_loader.load_item()