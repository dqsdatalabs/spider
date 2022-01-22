# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'doveproperties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source='Doveproperties_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.doveresidential.co.uk/residential/?filter%5Bproperty_type%5D=flat&filter%5Bbeds%5D=&filter%5Blocation%5D=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.doveresidential.co.uk/residential/?filter%5Bproperty_type%5D=house&filter%5Bbeds%5D=&filter%5Blocation%5D=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.doveresidential.co.uk/residential/?filter%5Bproperty_type%5D=room+available&filter%5Bbeds%5D=&filter%5Blocation%5D=",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'properties__listing__item__images clearfix')]"):
            status = item.xpath("./img/@src").get()
            if status and "let" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Doveproperties_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/strong/text()", get_num=True, input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'bed')]/text()", input_type="F_XPATH", split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            if "floor" in desc:
                floor = desc.split("floor")[0].strip().split(" ")[-1]
                item_loader.add_value("floor", floor.capitalize())

        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Deposit')]/text()", get_num=True, input_type="F_XPATH", split_list={"£":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH", split_list={"AVAILABLE":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'slick-nav')]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplan']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@href[contains(.,'maps')]", input_type="F_XPATH", split_list={"point=":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@href[contains(.,'maps')]", input_type="F_XPATH", split_list={"point=":1, ",":1})
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="DOVE RESIDENTIAL", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p/strong[@class='tel']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@doveproperties.co.uk", input_type="VALUE")
        room_count = response.xpath("//h1/text()").re_first(r'Room\s(\d)')
        if room_count:
            item_loader.add_value('room_count', room_count)
        yield item_loader.load_item()