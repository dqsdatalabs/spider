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
    name = 'premierpropertyni_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.premierpropertyni.com/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.premierpropertyni.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=6",
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

        for item in response.xpath("//ul[@class='property-list']/li/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Premierpropertyni_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        address = " ".join(response.xpath("//h1[@class='address']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
        
        city = response.xpath("//div[contains(@class,'property-address')]//span[contains(@class,'locality')]//text()").get()
        if city:
            item_loader.add_value("city", city)
            
        zipcode = response.xpath("//div[contains(@class,'property-address')]//span[contains(@class,'postcode')]//text()").get()
        if zipcode:
            zipcode = zipcode.replace(",","").strip()
            item_loader.add_value("zipcode", zipcode)
            
        floor = response.xpath("normalize-space(//td/text()[contains(.,'Floor')])").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        rent = "".join(response.xpath("//tr[th[contains(.,'Rent')]]/td//text()").getall())   
        if rent:                    
            if "week" in rent:                        
                rent = "".join(filter(str.isnumeric, rent.replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)*4)))   
            else:                        
                rent = rent.lower().split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')                        
                item_loader.add_value("rent", rent) 
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="normalize-space(//th[contains(.,'Deposit')]/following-sibling::td/text())", input_type="F_XPATH", get_num=True, split_list={"£":1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="normalize-space(//th[contains(.,'Available')]/following-sibling::td/text())", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="normalize-space(//th[contains(.,'Rent')]/following-sibling::td/text())", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="normalize-space(//th[contains(.,'Bedroom')]/following-sibling::td/text())", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="normalize-space(//th[contains(.,'Bathroom')]/following-sibling::td/text())", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="normalize-space(//th[contains(.,'Furnished')]/following-sibling::td/text()[contains(.,' furnished') or contains(.,'Furnished')])", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//section[contains(@class,'description')]//text()").getall())
        if desc:
            item_loader.add_value("description",  desc.strip())
        else:
            desc = " ".join(response.xpath("//section[@class='listing-additional-info']/p//text()").getall())
            if desc:
                item_loader.add_value("description",  desc.strip())

        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='jcarousel']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[@property='og:latitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[@property='og:longitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PREMIER PROPERTY SERVICES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 3752 7774", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@premierpropertyni.com", input_type="VALUE")

        yield item_loader.load_item()