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
    name = 'giantproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://giantproperty.co.uk/search/84732/page1/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://giantproperty.co.uk/search/84733/page1/",
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

        for item in response.xpath("//ul[@id='list']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Giantproperty_Co_PySpider_united_kingdom")
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'dprice')]/span[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/span[contains(.,'Bedroom')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='textbp']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "Feature" not in floor:
                item_loader.add_value("floor", floor)
        
        import dateparser
        from datetime import datetime
        available_date = ""
        if "available from" in desc.lower():
            available_date = desc.lower().split("available from")[1].strip().replace("contact",".").replace("we",".").split(".")[0].strip()
        elif "available late" in desc.lower():
            available_date = desc.lower().split("available late")[1].strip().split(".")[0].strip()
        elif "available" in desc.lower():
            available_date = desc.lower().split("available")[1].replace("call",",")
            if "immediately" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "," in available_date:
                available_date = available_date.split(",")[0].strip()
            
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        energy_label = response.xpath("//li/span[contains(.,'EPC')]/following-sibling::span/a/text()[contains(.,'D')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
            
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/span[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        if response.xpath("//script[contains(.,'LatLng(')]/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":0})
            ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":1,")":0})
        elif response.xpath("//script[contains(.,'LatLng')]/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
            ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='gallery']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="GIANT PROPERTY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9066 7873", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@giantproperty.co.uk", input_type="VALUE")

        yield item_loader.load_item()