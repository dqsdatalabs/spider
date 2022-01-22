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
    name = 'squarefootapartments_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "HTTPERROR_ALLOWED_CODES" : [404]
    }   
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://squarefootapartments.co.uk/search-properties?search-type=To+Let&property-type=Flat&bedrooms_min=&max-budget-let=2500",
                ],
                "property_type" : "apartment"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        # for item in response.xpath("//div[contains(@class,'image-holder')]/a/@href").extract():
        #     follow_url = response.urljoin(item)
        #     yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        for item in response.xpath("//div[@class='property-wrap']/div"):
            url = item.xpath(".//div[contains(@class,'image-holder')]/a/@href").extract_first()
            room = item.xpath(".//p[@class='beds-type']/text()[contains(.,'Bed')]").extract_first()
            follow_url = response.urljoin(url)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),"room":room})
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Squarefootapartments_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div/h1[@class='title']//text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip()) 
            item_loader.add_value("zipcode",title.split(",")[-1].strip()) 
            if "Road" not in title.split(",")[-2].strip():
                item_loader.add_value("city",title.split(",")[-2].strip()) 
        room = response.meta.get('room')
        if room:
            room_count = room.split("Bed")[0].strip()
            if room_count!="0":
                item_loader.add_value("room_count", room_count)
  
        rent = response.xpath("//div/h2[@class='title']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
    
        desc = " ".join(response.xpath("//div[@class='property-description']//p//text() | //div[@class='property-room-details']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if "furniture" in desc.lower():
                item_loader.add_value("furnished", True)
            if "parking" in desc.lower():
                item_loader.add_value("parking", True)

        images = [x for x in response.xpath("//div[@class='property-slider']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01274 392 971")
        item_loader.add_value("landlord_email", "hello@squarefootapartments.co.uk")
        item_loader.add_value("landlord_name", "Squarefoot Apartments")
        yield item_loader.load_item()
