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
    name = 'harperfinn_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://www.harpersofchiswick.com/properties/rent"]
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for follow_url in response.xpath("//div/a[strong]/@href").extract():
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//li[@class='page-item']/a[@rel='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/property/")[1].split("/")[0])

        prop_type = "".join(response.xpath("//div[@id='collapseOne']//p//text()").getall())
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower() or "maisonette" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        elif prop_type and "student" in prop_type.lower():
            item_loader.add_value("property_type", "student_apartment")
        else:
            return
        item_loader.add_value("external_source", "Harperfinn_Co_PySpider_united_kingdom")
        
        title = response.xpath("//h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("city", title.split(",")[-2].strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
            item_loader.add_value("address", title.strip())
       
        room_count = response.xpath("//ul[@class='features']/li[@class='bed']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)     
        
        bathroom_count=response.xpath("//ul[@class='features']/li[@class='bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_xpath("rent_string", "//span[@class='price']/text()")    
       
        desc = " ".join(response.xpath("//div[@id='collapseOne']//p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq M|sq m|Sq M|SqM|m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq))
                                          
        images = [response.urljoin(x) for x in response.xpath("//div[@id='carousel-thumbs']//li[@class='item']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@class='card-header' and contains(.,'Floor plan')]/a/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images) 
         
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 
            
        feature = response.xpath("//li/text()").get()
        if feature:
            if "Floor" in feature:
                item_loader.add_value("floor", feature.split("Floor")[0].strip())
            if "terrace" in feature.lower():
                item_loader.add_value("terrace", True) 

        item_loader.add_value("landlord_phone", "020 8995 2030")
        item_loader.add_value("landlord_name", "Harpers of Chiswick")  
        

        yield item_loader.load_item()
