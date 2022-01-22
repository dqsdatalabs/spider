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
    name = 'ulsterpropertysales_co_uk'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ulsterpropertysales.co.uk/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.ulsterpropertysales.co.uk/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
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

        for item in response.xpath("//div[@class='property-list']//div[contains(@class,'single-property')]/a[@class='property-link']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ulsterpropertysales_Co_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//tr/th[contains(.,'Address')]/following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='header-content']//span[@class='postcode']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//tr/th[contains(.,'Address')]/following-sibling::td//text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price-text']//text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//tr/th[contains(.,'Bedroom')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//tr/th[contains(.,'Bathroom')]/following-sibling::td//text()", input_type="F_XPATH", get_num=True)

        energy_label = response.xpath("//tr/th[contains(.,'EPC')]/following-sibling::td//a/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0])
        
        desc = " ".join(response.xpath("//div[@class='ListingDescr-text']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor" in desc:
            item_loader.add_value("floor", desc.split("floor")[0].strip().split(" ")[-1])
        
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//tr/th[contains(.,'Available')]/following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//tr/th[contains(.,'Style')]/following-sibling::td//text()[contains(.,'terrace') or contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//tr/th[contains(.,'Furnished')]/following-sibling::td//text()[not(contains(.,'Un'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='Slideshow-thumbs']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[@property='og:latitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[@property='og:longitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span/span[contains(.,'Deposit')]/following-sibling::span//text()", input_type="F_XPATH", get_num=True, split_list={"£":1," ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Ulster Property Sales", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(text(),'Call us now on')]/a/text()", input_type="F_XPATH")
        
        landlord_email = response.xpath("//script[contains(.,'Email us on')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.split('var lp ="')[-1].split('"')[0].strip() + "@" + landlord_email.split('var dp ="')[-1].split('"')[0].strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()