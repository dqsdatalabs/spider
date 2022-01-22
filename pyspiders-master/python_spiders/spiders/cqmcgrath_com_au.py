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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'cqmcgrath_com_au'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=Apartment&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=Duplex&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=House&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=Townhouse&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=Unit&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.cqmcgrath.com.au/renting/homes-for-rent/?keywords=&property_type=Studio&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=10000000",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='centerimage']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("/")[-2])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cqmcgrath_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'suburb')]//text() | //p[contains(@class,'street')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'suburb')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//p[contains(@class,'suburb')]//text() | //p[contains(@class,'street')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="/div[@class='copy']//text()[contains(.,'Land')]", input_type="F_XPATH", get_num=True, split_list={":":1, "m":0})
        
        if response.xpath("//span/i[contains(@class,'bed')]/following-sibling::text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span/i[contains(@class,'bed')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        elif response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span/i[contains(@class,'bath')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={"$":1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='item']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[@title='Floorplan']/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span/i[contains(@class,'car')]/following-sibling::text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[@class='name']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[contains(@class,'mobile')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="FOMRockhampton@mcgrath.com.au", input_type="VALUE")

        desc = " ".join(response.xpath("//div[@class='copy']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        yield item_loader.load_item()