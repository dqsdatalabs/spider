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
    name = 'firstnationalmh_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.firstnationalmh.com.au/real-estate-search/real-estate-for-rent?ltype=6&pype=512",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.firstnationalmh.com.au/real-estate-search/real-estate-for-rent?ltype=6&pype=1",
                    "https://www.firstnationalmh.com.au/real-estate-search/real-estate-for-rent?ltype=6&pype=2",
                    "https://www.firstnationalmh.com.au/real-estate-search/real-estate-for-rent?ltype=6&pype=67108864",
                    "https://www.firstnationalmh.com.au/real-estate-search/real-estate-for-rent?ltype=6&pype=2097152",
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
        for item in response.xpath("//div[@class='feat-links']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@class='np']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Firstnationalmh_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='extra']/p/strong[contains(.,'Address')]/following-sibling::text()", input_type="F_XPATH")
        
        zipcode = response.xpath("//div[@class='extra']/p/strong[contains(.,'Postcode')]/following-sibling::text()").get()
        if zipcode:
            zipcode = "VIC " + zipcode.strip().split(" ")[0]
            item_loader.add_value("zipcode", zipcode)

        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='extra']/p/strong[contains(.,'Suburb')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='ld-top']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='desc']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@class='bed']/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@class='bath']/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='extra']/p/strong[contains(.,'Price')]/following-sibling::text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='extra']/p/strong[contains(.,'Date')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='extra']/p/strong[contains(.,'Bond')]/following-sibling::text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='extra']/p/strong[contains(.,'ID')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='Gallery']//@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[@class='car']/strong/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//a[@class='name']/span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//a[contains(@class,'mobile')]/span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="admin@firstnationalmh.com.au", input_type="VALUE")

        yield item_loader.load_item()