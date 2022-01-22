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
    name = 'bekdonrichards_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://www.bekdonrichards.com.au/properties-for-lease/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item-wrap']/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@class='next_page_link']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[@class='desc']//h2//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='desc']//p//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bekdonrichards_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='street-address']/text() | //div[@class='suburb']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='suburb']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='suburb']/text()", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='headline-property']/h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@class='property_description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(@class,'bedroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@class,'bathroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='price']//text()[contains(.,'Bond')]", input_type="F_XPATH", get_num=True, split_list={"$":1, ")":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"/":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='cycle-slideshow']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker([')]//text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker([')]//text()", input_type="F_XPATH", split_list={"L.marker([":1, ",":1,"]":0})
        if response.xpath("//li[contains(@class,'car')]/text()[.!='0'] | //div[@class='block_content']//span/text()[contains(.,'Parking')]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@class,'car')]/text()[.!='0'] | //div[@class='block_content']//span/text()[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        elif response.xpath("//p[contains(@class,'property_description')]//text()[contains(.,'Car')]").get():
            item_loader.add_value("parking", True)        
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='block_content']//span/text()[contains(.,'Balcon') or contains(.,'balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='block_content']//span/text()[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='block_content']//span/text()[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='block_content']//span/text()[contains(.,'Terrace') or contains(.,' terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@class='block_content']//span/text()[contains(.,'Pool') or contains(.,'pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='agent_contact_info']/h4//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[@class='agent_phone']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiries@bekdonrichards.com.au", input_type="VALUE")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None