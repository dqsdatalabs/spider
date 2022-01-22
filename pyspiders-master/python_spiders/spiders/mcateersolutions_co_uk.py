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
    name = 'mcateersolutions_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Mcateersolutions_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_url = "https://www.mcateersolutions.co.uk/property-for-rent"

        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='maincontent']/div/div/div"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            is_agreed = item.xpath(".//div[@class='propagree' and contains(.,'Agreed')]").get()
            property_type = item.xpath(".//div[@class='propic border-0' and not(contains(.,'Commercial'))]/text()").get()
            if not is_agreed and property_type:
                if get_p_type_string(property_type):
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-3})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='fnum']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/span[contains(.,'Bedroom')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/span[contains(.,'Bathroom')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/span[contains(.,'Available')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Car Park')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Deposit')]/text()", input_type="F_XPATH", get_num=True, split_list={"£":1})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='prop-slideshow']//@href", input_type="M_XPATH")
        
        desc = " ".join(response.xpath("//div[contains(@class,'information')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="McAteer Solutions", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 7965 9444", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@mcateersolutions.co.uk", input_type="VALUE")

        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None