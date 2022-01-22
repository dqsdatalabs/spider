# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from itemadapter.utils import is_scrapy_item
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider): 
    name = 'nexaproperties'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Nexaproperties_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.nexaproperties.com/properties/?status=tolet",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//div[@class='w-full md:w-1/2 px-3 pb-6 property']//a"):
            property_type=item.xpath(".//span[@class='uppercase']/text()").get()

            yield Request(response.urljoin(item.xpath(".//@href").get()), callback=self.populate_item, meta={"property_type":property_type}) 
            seen = True
        if page == 2 or seen:
            next_page = f"https://www.nexaproperties.com/properties/page/{page}/?status=tolet" 
            if next_page:
                yield Request(url=response.urljoin(next_page),callback=self.parse) 

    
    
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type=response.meta.get("property_type") 

        if "apartment" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if "house" in property_type.lower():
            item_loader.add_value("property_type","house")
        item_loader.add_xpath("title","//title//text()")
        adres=item_loader.get_output_value("title")
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("city",adres.split(",")[-1].split("-")[0])
        external_id=response.xpath("//span[.='Property ID: ']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        description="".join(response.xpath("//h3[.='Description']/following-sibling::div//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//span[.='Per Month']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[-1].strip().replace(",",""))
        item_loader.add_value("currency","EUR")

        room_count=response.xpath("//div[contains(.,'Beds')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[contains(.,'Baths')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=[x for x in response.xpath("//div[@id='gallery-nav']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//div[@id='map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//div[@id='map']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        name=response.xpath("//span[.='Agent']/parent::h2/following-sibling::div//h3/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//span[.='Agent']/parent::h2/following-sibling::div//span[.='Mobile: ']/following-sibling::span/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//span[.='Agent']/parent::h2/following-sibling::div//span[.='Email: ']/following-sibling::span/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
        namecheck=item_loader.get_output_value("landlord_name")
        if not namecheck:
            item_loader.add_value("landlord_name","NEXA Portsmouth")
        
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone","+44 (0) 2392 295046")
        emailcheck=item_loader.get_output_value("landlord_email")
        if not emailcheck:
            item_loader.add_value("landlord_email","hello@nexaproperties.com")
            
        yield item_loader.load_item()