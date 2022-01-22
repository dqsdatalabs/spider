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
    name = 'country_estates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.country-estates.com/search?sta=toLet&st=rent&pt=residential&stygrp=3&stygrp=6",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.country-estates.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=9",
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
        for item in response.xpath("//a[contains(@class,'btn btn-more')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(@class,'paging-next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("/")[-1])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Country_Estates_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[@class='address']//text()", input_type="M_XPATH", replace_list={"\n":""})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@class='postcode']/text()", input_type="F_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='locality'][2]/text()", input_type="F_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[@class='address']//text()", input_type="M_XPATH", replace_list={"\n":""})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='property-description']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//tr/td[contains(.,'Bedroom')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//tr/td[contains(.,'Bathroom')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//tr/td[contains(.,'Rent')]/following-sibling::td/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//tr/td[contains(.,'Available')]/following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//tr/td[contains(.,'Deposit')]/following-sibling::td/span/text()", input_type="F_XPATH", get_num=True, split_list={".":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slideshow-thumbs']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[@property='og:latitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[@property='og:longitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//tr/td[contains(.,'Furnished')]/following-sibling::td//text()[not(contains(.,'Un'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//tr/td[contains(.,'Style')]/following-sibling::td//text()[contains(.,'terrace') or contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Country Estates", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="442893340726", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="ballyclare@country-estates.com", input_type="VALUE")
        
        energy_label = response.xpath("//tr/td[contains(.,'EPC')]/following-sibling::td//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0])
        
        yield item_loader.load_item()