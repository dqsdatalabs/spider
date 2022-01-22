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
    name = 'brian_cox_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.brian-cox.co.uk/property/?department=residential-lettings&address_keyword=&property_type=22&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=0&maximum_bedrooms=&view=&pgp=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.brian-cox.co.uk/property/?department=residential-lettings&address_keyword=&property_type=9&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=0&maximum_bedrooms=&view=&pgp=",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='view-details']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'next ')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            ) 
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if response.url == "https://www.brian-cox.co.uk/property/":
            return
        item_loader.add_value("external_source", "Brian_Cox_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'?p=')")
        
        
        title = response.xpath("//h1[@class='property_title']/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div[@class='pro-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = response.xpath("//div[@class='pro-price']/text()").get()
        if rent:
            if rent != "£0":
                item_loader.add_value("rent_string", rent.replace(",",""))
            else:
                item_loader.add_value("currency", "GBP")
                
        bathroom_count = response.xpath("//li[contains(.,'Bathrooms:')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'summary-inner')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        room_count = response.xpath("//li[contains(.,'Bedrooms:')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
            
        images = [x for x in response.xpath("//div[@id='slider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        
        furnished = response.xpath("//li[contains(.,'Furnished:') or contains(.,'Furnished')]//text()").get()
        if furnished:
            if ":" in furnished:
                furnished = furnished.split(":")[1].strip()
                if "Furnished" in furnished:
                    item_loader.add_value("furnished", True)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished", True)
        
        from datetime import datetime
        available_date = response.xpath("//li[contains(.,'Available') and not(contains(.,'Availability:'))]//text()").get()
        if available_date:
            if "Immediately" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon') or contains(.,'balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        item_loader.add_value("landlord_name", "BRIAN COX ESTATE AGENTS")
        item_loader.add_xpath("landlord_phone", "//div[@class='phone']//text()")
        item_loader.add_value("landlord_email", "paul.budd@brian-cox.co.uk")
        
        yield item_loader.load_item()