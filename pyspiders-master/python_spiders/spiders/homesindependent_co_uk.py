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
    name = 'homesindependent_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Apartment&letagreed=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Detached&letagreed=1",
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Detached+Bungalow&letagreed=1",
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=End+Terrace&letagreed=1",
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Semi-Detached&letagreed=1",
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Terrace&letagreed=1",
                    "https://www.homesindependent.co.uk/rentals?keywords=&pricemin=&pricemax=&area=&beds=&receptions=&housetype=Townhouse&letagreed=1",
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

        for item in response.xpath("//div[contains(@class,'properties-list')]/div[contains(@class,'box')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Homesindependent_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        address = response.xpath("//div[@class='property-intro']/h1/text()").get()
        city = response.xpath("//div[@class='property-intro']/h3/text()").get()
        if address or city:
            item_loader.add_value("address", address+city)
            item_loader.add_value("city", city.split(",")[0].strip())
            item_loader.add_value("zipcode", city.split(",")[-1].strip())
        
        desc = " ".join(response.xpath("//div[@class='main']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedroom') or contains(.,'bedroom')]/text()", input_type="F_XPATH", get_num=True, split_list={"Bed":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/span[contains(.,'Rent')]/text()", input_type="F_XPATH", get_num=True, split_list={"£":1,"pm":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'terrace')]/text()", input_type="F_XPATH", tf_item=True)
        
        floor = response.xpath("//li[contains(.,'floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("floor")[0].strip().split(" ")[-1])
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//section[contains(@class,'property-gallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Homes Independent", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 2565 1111", input_type="VALUE")
        item_loader.add_value("landlord_email", "admin@homesindependent.co.uk")

        yield item_loader.load_item()