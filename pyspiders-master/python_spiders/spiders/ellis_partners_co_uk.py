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
    name = 'ellis_partners_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source='Ellis_Partners_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.ellis-partners.co.uk/property-search/?category=residential&property-id=&status=let&location=any&type=flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any",
                    "http://www.ellis-partners.co.uk/property-search/?category=residential&property-id=&status=let&location=any&type=ground-floor-flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.ellis-partners.co.uk/property-search/?category=residential&property-id=&status=let&location=any&type=studio-flat&bedrooms=any&bathrooms=any&min-price=any&max-price=any",
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
        for item in response.xpath("//div[@class='detail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[.='Next »']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ellis_Partners_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        title = response.xpath("//div[@class='wrap clearfix']/p//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div/h4[contains(.,'Address')]//following-sibling::ul/li/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div/h4[contains(.,'Address')]//following-sibling::ul/li/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div/h4[contains(.,'Address')]//following-sibling::ul/li/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//i[contains(@class,'bed')]//following-sibling::text()", input_type="F_XPATH", split_list={"\xa0":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h5[@class='price']/span[2]/text()", get_num=True, input_type="F_XPATH", split_list={"PCM":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//i[contains(@class,'bath')]//following-sibling::text()", input_type="F_XPATH", split_list={"\xa0":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h4[contains(.,'Property ID')]/text()", input_type="F_XPATH", split_list={":":1})
        
        desc = " ".join(response.xpath("//article/div[contains(@class,'content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//article/div[contains(@class,'content')]//text()[contains(.,'Deposit')]", input_type="F_XPATH", split_list={"£":1})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//article/div[contains(@class,'content')]//text()[contains(.,'AVAILABLE')]", input_type="F_XPATH", split_list={"AVAILABLE":1, "*":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Ellis & Partners", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//li[@class='office']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="bhresi@ellis-partners.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()