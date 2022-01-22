# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'collectionapartments_com'
    execution_type='testing'
    country='netherlands'
    locale='en'
    external_source='Collectionapartments_PySpider_netherlands'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hausing.com/properties-for-rent-amsterdam",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'link-post')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//h6[@class='subheading-2']/text()").get()
        if dontallow and "reserved" in dontallow.lower():
            return 
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//div[@class='content-width-medium']//h1/text()")
        item_loader.add_xpath("address", "//div[@class='content-width-medium']//h1/text()")
        item_loader.add_value("city","Amsterdam")

        rent = "".join(response.xpath("//div[@class='post-meta-left']/h6/text()").extract())
        if rent:
            item_loader.add_value("rent_string",rent )
      
        deposit = "".join(response.xpath("//strong[.='Deposit']/following-sibling::text() | //strong[.='Deposit: ']/following-sibling::text() | //p[contains(.,'Deposit')]/text()").extract())
        if deposit:
            if "month" in deposit:
                dep =  re.findall("\d+",deposit)
                if rent:
                    price = rent.split("€")[1].split("/")[0].strip()
                    price=re.findall("\d+",price)
                    if price:
                        item_loader.add_value("deposit",int(dep[0])*int(price[0]))
            if ":" in deposit:
                dep =  re.findall("\d+",deposit)
                if rent:
                    price = rent.split("€")[1].split("/")[0].strip()
                    price=re.findall("\d+",price)
                    if price:
                        item_loader.add_value("deposit",int(dep[0])*int(price[0]))



   
        item_loader.add_xpath("room_count", "//div[@class='text-block-15'][1]/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='text-block-15'][2]/text()")
        item_loader.add_xpath("square_meters", "//div[@class='text-block-15'][3]/text()")

        desc = "".join(response.xpath("//div[@class='text-block-16 text-left']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='w-slide']//@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)    

      
        listed = " ".join(response.xpath("//li/text()").extract())
        if listed:
            if "Washing machine" in listed:
                item_loader.add_value("washing_machine",True) 
            if "balcony" in listed:
                item_loader.add_value("balcony",True) 

        furnished = " ".join(response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]/text()").extract())
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            if "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)

     
        item_loader.add_value("landlord_phone", "+31 (0) 20 244 2918")
        item_loader.add_value("landlord_email", "hello@hausing.com")
        item_loader.add_value("landlord_name", "Hausing")


        yield item_loader.load_item()