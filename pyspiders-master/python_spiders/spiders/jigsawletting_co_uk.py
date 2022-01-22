# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'jigsawletting_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Bungalow",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+End+Terrace",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Mid+Terrace",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Semi-Detached",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House+-+Townhouse",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi+Detached",
                    "https://www.jigsawletting.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Terraced",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='col-lg-6']/h2/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = f"https://www.jigsawletting.co.uk/search/{page}.html?" + base_url.split("?")[1]
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])
        item_loader.add_value("external_source", "Jigsawletting_Co_PySpider_united_kingdom")     
   
        title = response.xpath("//div/h1//text()").extract_first()
        if title:
            item_loader.add_value("title", title) 
        address = response.xpath("//div/h1//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())        
            item_loader.add_value("city",address.split(",")[-1].strip())        

        rent = " ".join(response.xpath("//div/h2//text()").extract())
        if rent:       
            item_loader.add_value("rent_string",rent.strip()) 

        room_count = response.xpath("//div[@class='room-icons']/span[svg[@class='icon--bedrooms']]/strong/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count)
                  
        desc = " ".join(response.xpath("//div[contains(@class,'hidden-xs')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
    
        script_map = response.xpath("//script[@type='application/ld+json']//text()[contains(.,'latitude')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('"latitude": "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", script_map.split('"longitude": "')[1].split('"')[0].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-carousel']/div[@class='carousel-inner']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "Jigsaw Letting")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'property-contact-box')]/p[@class='property-tel']//a/text()")
        item_loader.add_value("landlord_email", "info@jigsawletting.co.uk")
        yield item_loader.load_item()