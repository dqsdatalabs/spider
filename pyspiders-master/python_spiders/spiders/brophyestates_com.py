# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
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
    name = 'brophyestates_com'
    execution_type='testing'
    country='Ireland'
    locale='en'
    external_source = "Brophyestates_PySpider_Ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.brophyestates.com/search/for/rent/",
                ],
                "property_type" : "apartment",
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[text()='find out more >']/@href").getall():
            follow_url = item

    
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        images = response.xpath("//a[@data-fancybox='gallery']/@href").getall()
        if images:
            item_loader.add_value("images",images)

        rent = response.xpath("//div[@class='title'][text()='Price']/following::div/text()").get()
        if rent:
            rent = rent.replace(",","").replace("€","")

        item_loader.add_value("external_link", response.url)

        address = response.xpath("//div[@class='bed-type']/text()").get()
        if address:
            item_loader.add_value("address",address)

        bathroom = response.xpath("//div[text()='Bathrooms']/following-sibling::div/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom)

        room = response.xpath("//div[text()='Bedrooms']/following-sibling::div/text()").get()
        if room:
            item_loader.add_value("room_count",room)

        item_loader.add_value("city","Dublin")
        item_loader.add_value("currency","EUR")


        prop_type = response.xpath("//div[text()='Property Type']/following-sibling::div/text()").get()
        if prop_type:
            if "apartment" in prop_type.lower():
                item_loader.add_value("property_type","apartment")
            elif "house" in prop_type.lower():
                item_loader.add_value("property_type","house")
            elif "room" in prop_type.lower():
                item_loader.add_value("propety_type","room")
            elif "studio" in prop_type.lower():
                item_loader.add_value("property_type","studio")

        desc = " ".join(response.xpath("//div[@class='read-more-func']/p[not(preceding-sibling::div)]").getall())
        if desc:
            item_loader.add_value("description",desc)

        facilities = str(response.xpath("//table[@id='facilities']//li").getall()).lower()
        if "dishwasher" in facilities:
            item_loader.add_value("dishwasher",True)

        if "parking" in facilities:
            item_loader.add_value("parking",True)

        if "washing" in facilities:
            item_loader.add_value("washing_machine",True)

        if "balcony" in facilities:
            item_loader.add_value("balcony",True)
        
        if "parking" in facilities:
            item_loader.add_value("parking",True)
        
        if "elevator" in facilities:
            item_loader.add_value("elevator",True)

        
        external_id = response.xpath("//div[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.split(":")[-1].strip()
            item_loader.add_value("external_id",external_id)
        item_loader.add_value("external_source",self.external_source)


        item_loader.add_value("landlord_name","BROPHY ESTATES")
        item_loader.add_value("landlord_email","info@brophyestates.com")
        item_loader.add_value("landlord_phone","+353 1 845 7988")

        rent = response.xpath("//div[text()='Price']/following-sibling::div/text()").get()
        if rent:
            rent = rent.replace(",","").replace("€","").replace(" ","")
            item_loader.add_value("rent",rent)

        title = response.xpath("//div[@class='bed-type']/text()").get()
        if title:
            item_loader.add_value("title",title)

        yield item_loader.load_item()