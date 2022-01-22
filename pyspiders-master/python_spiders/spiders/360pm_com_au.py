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
    name = '360pm_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    custom_settings = {
        "PROXY_ON" : "True",
        "PASSWORD" : "wmkpu9fkfzyo",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Apartment",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Flat",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Unit",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Duplex",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Semi-detached",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=House",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Terrace",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Townhouse",
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Villa",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.360pm.com.au/rent-mackay?search=&listing_type=rent&property_type=Studio",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='figure']/@href").getall():           
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="360pm_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='inner_desc']/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='inner_desc']/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='inner_desc']/h3/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={"pw":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='inner_desc']//i[contains(@class,'person1')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='inner_desc']//span[contains(@class,'shower')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'prop-desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        item_loader.add_value("external_id", response.url.split("id=")[1].split("/")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='inner_desc']//i[contains(@class,'car')]/following-sibling::text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//h3[contains(.,'Available Date')]/following-sibling::ul/li//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@id='tab-features']//li[contains(.,'Pool')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@id='tab-features']//li[contains(.,'Balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[@id='tab-features']//li[contains(.,'Dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-wrap']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'card')]//div[@class='title']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="substring-after(//div[contains(@class,'card')]//li/a/@href[contains(.,'tel')], ':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="manager@360pm.com.au", input_type="VALUE")
        
        yield item_loader.load_item()