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
    name = 'mcgranaghanestateagents_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = 'Mcgranaghanestateagents_PySpider_united_kingdom'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.mcgranaghanestateagents.com/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mcgranaghanestateagents.com/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'BoxList')]/li/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='Paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
       
        item_loader.add_value("external_id", response.url.split("/")[-1])
        address = " ".join(response.xpath("//h1[contains(@class,'Address')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Mcgranaghanestateagents_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[contains(@class,'Town')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//span[contains(@class,'Out')]//text() |//h1//span[contains(@class,'In')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@class,'bed')]/span/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//th[contains(.,'Bathroom')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//th[contains(.,'Rent')]/following-sibling::td/span[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//th[contains(.,'Furnished')]/following-sibling::td/text()[not(contains(.,'Un'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li/span[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='Slideshow-thumbs']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@data-map-options", input_type="F_XPATH",split_list={'lat":':1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@data-map-options", input_type="F_XPATH",split_list={'lng":':1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//th[contains(.,'Available')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//th[contains(.,'Style')]/following-sibling::td/text()[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        
        floor = response.xpath("//li/span[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])

        deposit = response.xpath("normalize-space(//table//tr[th[.='Deposit']]/td/text())").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip())
            
        desc = " ".join(response.xpath("//div[contains(@class,'Descr-text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Mc Granaghan Estate Agents", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="+442890309030", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@mcgranaghanestateagents.com", input_type="VALUE")

        yield item_loader.load_item()