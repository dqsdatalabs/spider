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
    name = 'jameslaurenceuk_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Jameslaurenceuk_PySpider_united_kingdom"
    start_urls = ["https://jameslaurenceuk.com/search_results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword=&radius=&availability="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='thumbnail']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            next=response.xpath("//a[@class='next page-numbers']/@href").get()
            if next:
                yield Request(next,callback=self.parse,meta={"page":page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
                
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='single-title block']/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("?p=")[1].strip())

        description=response.xpath("//div[@class='desc-main']/p/text()").get()
        if description:
            item_loader.add_value("description",description)

        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))

        adres=response.xpath("//div[@class='details-area']//div[@class='single-title block']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//div[@class='details-area']//div[@class='single-title block']/text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1])
        zipcode=response.xpath("//div[@class='details-area']//div[@class='single-address block']/p/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        price=response.xpath("//div[@class='details-area']//div[@class='price block']/text()").get()
        if price:
            item_loader.add_value("rent",price.replace("£","").strip())
        item_loader.add_value("currency", "GBP")
        
        room_count=response.xpath("//p[@class='bedroom']/following-sibling::p/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//p[@class='bathroom']/following-sibling::p/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=[x for x in response.xpath("//ul[@class='slides']//li//a/@href").getall()]
        if images:
            item_loader.add_value("images",images)

        item_loader.add_value("landlord_name","James Laurence")
        item_loader.add_value("landlord_email","lettings@jameslaurenceuk.com")
        item_loader.add_value("landlord_phone","0121 604 4060")
        

        map_iframe = response.xpath("//iframe[contains(@src,'google.com/maps?')]/@src").get()
        if map_iframe: yield Request(map_iframe, callback=self.get_map, dont_filter=True, meta={"item_loader": item_loader})
        else: yield item_loader.load_item()
    
    def get_map(self, response):
        item_loader = response.meta["item_loader"]
        latitude = response.xpath("//div[@id='mapDiv']/following-sibling::script[1]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('",null,[null,null,')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('",null,[null,null,')[1].split(',')[1].split(']')[0].strip())
   
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
        