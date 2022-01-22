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
    name = 'pyperproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pyperproperty.com/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pyperproperty.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='PropBox-content']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Pyperproperty_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//th[contains(.,'Address')]//following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[contains(@class,'addressTown')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//span[contains(@class,'addressPostcode')]//text()", input_type="M_XPATH", replace_list={" \n":""})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'ListingDescr')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//th[contains(.,'Bed')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//th[contains(.,'Bath')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//th[contains(.,'Rent')]//following-sibling::td//span[contains(@class,'price-text')]//text()", input_type="F_XPATH", get_num=True, replace_list={"£":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//th[contains(.,'Deposit')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True, replace_list={"£":""," ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
 
        furnished = response.xpath("//th[contains(.,'Furnished')]//following-sibling::td//text()").get()
        if furnished and "unfurnished" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//th[contains(.,'Available')]//following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[contains(@rel,'photograph')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[contains(@property,'og:latitude')]//@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[contains(@property,'og:longitude')]//@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'Garage')]//text() | //p[contains(.,'Parking') or contains(.,'Garage') or contains(.,'Allocated parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PYPER PROPERTY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9335 1232", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@pyperproperty.com", input_type="VALUE")
  
        
        yield item_loader.load_item()