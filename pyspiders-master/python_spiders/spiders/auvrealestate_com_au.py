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
    name = 'auvrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source='Auvrealestate_Com_PySpider_australia'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://auvrealestate.com.au/search/?status=for-lease&type=apartment",
                    "http://auvrealestate.com.au/search/?status=for-lease&type=unit",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://auvrealestate.com.au/search/?status=for-lease&type=house",
                    "http://auvrealestate.com.au/search/?status=for-lease&type=townhouse",
                    "http://auvrealestate.com.au/search/?status=for-lease&type=villa",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.auvrealestate.com.au/rental/rental-properties"
                ],
                "property_type" : "apartment"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'ere-item-wrap')]//h4/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Auvrealestate_Com_PySpider_australia")

        title = response.xpath("//div[contains(@class,'property-heading')]//h4//text()").get()
        if title:
            item_loader.add_value("title", title)
            if "furnished" in title.lower():
                item_loader.add_value("furnished", True)
        

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        address = response.xpath("//div[contains(@class,'property-address')]//span//text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.strip().split(" ")[-1]
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = response.xpath("//li[contains(.,'ZIP')]//span//text()").get()
                if zipcode:
                    item_loader.add_value("zipcode", zipcode)
        
        if "vic" in address.lower():
            city = address.split(",")[1].split(",")[0]
        else:                
            city = address.split(",")[1]
        item_loader.add_value("city", city)

        rent = response.xpath("//li//span[contains(@class,'property-price')]//text()").get()
        if rent:
            price = rent.strip().replace(",","").split("$")[1]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "USD")
        square_meters = response.xpath("//li[contains(.,'Size')]//span//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])

        room_count = response.xpath("//li[contains(.,'Bed')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Bath')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//li[contains(.,'Garage')]//span//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        images = [x for x in response.xpath("//div[contains(@class,'property-gallery-item')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[contains(@class,'agent-heading')]//a//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent-email')]//span//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent-mobile')]//span//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()