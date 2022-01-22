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
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'houselet_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        start_url = "https://www.houselet.ie/property-to-rent"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Property Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = "".join(response.xpath("//div[@class='details-information']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2].split("-")[-1])
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Houselet_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/a/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/a/text()", input_type="M_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/a/text()", input_type="M_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='priceask']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@class='details-information']/p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            
        from word2number import w2n
        if "bedroom" in desc:
            room_count = desc.split("bedroom")[0].strip().split(" ")[-1].replace("-","")
            item_loader.add_value("room_count", w2n.word_to_num(room_count))

        bathroom = response.xpath("//div[@class='bullets-li']/p[contains(.,'Bathroom')]/text()").get()
        if bathroom:
            bathroom = bathroom.strip().split(" ")[0]
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except: pass
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='sp-slides']//@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='bullets-li']/p[contains(.,'Parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='bullets-li']/p[contains(.,'Furnished') or contains(.,' furnished')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='bullets-li']/p[contains(.,'Washing')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]//text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]//text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="HouseLet", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="353 1 6272768", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@houselet.ie", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None