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
    name = 'loverealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.loverealty.com.au/properties/houses-for-rent-newcastle?suburb=&type=apartment&min_price=0&max_price=0&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.loverealty.com.au/properties/houses-for-rent-newcastle?suburb=&type=unit&min_price=0&max_price=0&sort=",
                    "https://www.loverealty.com.au/properties/houses-for-rent-newcastle?suburb=&type=townhouse&min_price=0&max_price=0&sort=",
                    "https://www.loverealty.com.au/properties/houses-for-rent-newcastle?suburb=&type=house&min_price=0&max_price=0&sort=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'property-features')]//a[contains(@class,'btn-submit')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]}) 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Loverealty_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        
        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", f"NSW {zipcode.split(',')[-1].strip()}")
        
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-3})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/p[contains(@class,'h6 pt')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//img[contains(@src,'bed')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//img[contains(@src,'bath')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/p[contains(.,'Price')]/following-sibling::p/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={"$":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img[contains(@class,'img-fluid')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//img[contains(@src,'car')]/following-sibling::span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="LOVE REALTY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 4958 8555", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="rentals@loverealty.com.au", input_type="VALUE")

        yield item_loader.load_item()
